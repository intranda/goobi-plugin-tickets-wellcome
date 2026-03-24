package org.goobi.api.mq;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.goobi.beans.GoobiProperty;
import org.goobi.beans.GoobiProperty.PropertyOwnerType;
import org.goobi.beans.Process;
import org.goobi.beans.Step;
import org.goobi.production.enums.LogType;
import org.goobi.production.enums.PluginReturnValue;

import de.sub.goobi.config.ConfigurationHelper;
import de.sub.goobi.helper.Helper;
import de.sub.goobi.helper.S3FileUtils;
import de.sub.goobi.helper.StorageProvider;
import de.sub.goobi.persistence.managers.ProcessManager;
import de.sub.goobi.persistence.managers.PropertyManager;
import lombok.extern.log4j.Log4j2;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.UploadPartCopyResponse;

/**
 * 
 * This class is used to import video data from s3 upload storage into a given process. The upload is considered as complete if the process contains
 * either a jpg + mpg or a jpg + mp4 + mxf file. If the upload was completed, the current open step gets closed.
 * 
 */

@Log4j2
public class ImportVideoDataHandler implements TicketHandler<PluginReturnValue> {

    @Override
    public PluginReturnValue call(TaskTicket ticket) {

        String bucket = ticket.getProperties().get("bucket");

        String s3Key = ticket.getProperties().get("s3Key");
        Path destinationFolder = Paths.get(ticket.getProperties().get("destination"));

        log.debug("copy {} to {}", bucket + "/" + s3Key, destinationFolder);

        S3FileUtils utils = (S3FileUtils) StorageProvider.getInstance();
        S3AsyncClient s3 = utils.getS3();

        Process process = ProcessManager.getProcessById(ticket.getProcessId());

        // check process status
        boolean uploadIsAllowed = false;
        Step currentStep = process.getAktuellerSchritt();
        if (currentStep != null) {
            // no open step found, abort

            switch (currentStep.getTitel()) {
                case "Bibliographic import":
                case "Video data import":
                case "Audio (Video) data import":
                case "Audio data import":
                case "Document import":
                case "AV file upload":
                case "Image upload":
                case "Import data":
                case "JP2 upload":
                case "PDF upload":
                    //                upload is allowed
                    uploadIsAllowed = true;
                    break;

                default:
                    // process is in a different state, abort
                    break;
            }
        }
        // delete and abort, if upload isn't allowed

        if (!uploadIsAllowed) {
            // log entry
            Helper.addMessageToProcessJournal(ticket.getProcessId(), LogType.ERROR, "File import aborted, process has not the correct status.",
                    "ticket");
            List<ObjectIdentifier> toDelete = new ArrayList<>();
            toDelete.add(ObjectIdentifier.builder()
                    .key(s3Key)
                    .build());

            DeleteObjectsRequest dor = DeleteObjectsRequest.builder()
                    .bucket(bucket)
                    .delete(Delete.builder()
                            .objects(toDelete)
                            .build())
                    .build();

            s3.deleteObjects(dor);
            log.info("deleted file {} from bucket", s3Key);
            return PluginReturnValue.ERROR;
        }

        int index = s3Key.lastIndexOf('/');
        Path destinationFile;
        if (index != -1) {
            destinationFile = destinationFolder.resolve(s3Key.substring(index + 1));
        } else {
            destinationFile = destinationFolder.resolve(s3Key);
        }

        String destBucket = ConfigurationHelper.getInstance().getS3Bucket();
        String destKey = S3FileUtils.path2Key(destinationFile);

        long objectSize = s3.headObject(r -> r.bucket(bucket).key(s3Key)).join().contentLength();
        long fiveMB = 5L * 1024 * 1024;

        if (objectSize <= fiveMB) {
            s3.copyObject(b -> b.sourceBucket(bucket).sourceKey(s3Key).destinationBucket(destBucket).destinationKey(destKey)).join();
        } else {
            copyMultipart(s3, bucket, s3Key, destBucket, destKey, objectSize);
        }

        List<GoobiProperty> properties = PropertyManager.getPropertiesForObject(process.getId(), PropertyOwnerType.PROCESS);
        if (!properties.stream().anyMatch(pp -> "s3_import_bucket".equals(pp.getPropertyName()))) {
            addProcesspropertyToProcess(process, "s3_import_bucket", bucket);
        }
        if (!properties.stream().anyMatch(pp -> "s3_import_prefix".equals(pp.getPropertyName()))) {
            String prefix = s3Key.substring(0, s3Key.lastIndexOf('/'));
            addProcesspropertyToProcess(process, "s3_import_prefix", prefix);
        }

        String deleteFiles = ticket.getProperties().get("deleteFiles");
        if (StringUtils.isNotBlank(deleteFiles) && "true".equalsIgnoreCase(deleteFiles)) {
            List<ObjectIdentifier> toDelete = new ArrayList<>();
            toDelete.add(ObjectIdentifier.builder()
                    .key(s3Key)
                    .build());
            DeleteObjectsRequest dor = DeleteObjectsRequest.builder()
                    .bucket(bucket)
                    .delete(Delete.builder()
                            .objects(toDelete)
                            .build())
                    .build();

            s3.deleteObjects(dor);
            log.info("deleted file from bucket");
        }

        return PluginReturnValue.FINISH;
    }

    private void copyMultipart(S3AsyncClient s3, String srcBucket, String srcKey, String destBucket, String destKey, long objectSize) {
        long partSize = 64L * 1024 * 1024; // 64 MB
        String uploadId = s3.createMultipartUpload(b -> b.bucket(destBucket).key(destKey)).join().uploadId();
        try {
            List<CompletedPart> completedParts = new ArrayList<>();
            long offset = 0;
            int partNumber = 1;
            while (offset < objectSize) {
                long end = Math.min(offset + partSize - 1, objectSize - 1);
                String range = "bytes=" + offset + "-" + end;
                int pn = partNumber;
                UploadPartCopyResponse response = s3.uploadPartCopy(b -> b
                        .sourceBucket(srcBucket)
                        .sourceKey(srcKey)
                        .destinationBucket(destBucket)
                        .destinationKey(destKey)
                        .uploadId(uploadId)
                        .partNumber(pn)
                        .copySourceRange(range))
                        .join();
                completedParts.add(CompletedPart.builder().partNumber(pn).eTag(response.copyPartResult().eTag()).build());
                offset += partSize;
                partNumber++;
            }
            s3.completeMultipartUpload(b -> b
                    .bucket(destBucket)
                    .key(destKey)
                    .uploadId(uploadId)
                    .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build()))
                    .join();
        } catch (Exception e) {
            s3.abortMultipartUpload(b -> b.bucket(destBucket).key(destKey).uploadId(uploadId)).join();
            throw e;
        }
    }

    private void addProcesspropertyToProcess(Process process, String name, String value) {
        GoobiProperty pp = new GoobiProperty(PropertyOwnerType.PROCESS);
        pp.setOwner(process);
        pp.setPropertyName(name);
        pp.setPropertyValue(value);

        PropertyManager.saveProperty(pp);
    }

    @Override
    public String getTicketHandlerName() {
        return "importVideoData";
    }

}
