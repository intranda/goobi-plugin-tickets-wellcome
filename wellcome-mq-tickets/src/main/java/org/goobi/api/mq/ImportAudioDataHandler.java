package org.goobi.api.mq;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.lang.StringUtils;
import org.goobi.production.enums.PluginReturnValue;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;

import de.sub.goobi.config.ConfigurationHelper;
import de.sub.goobi.helper.S3FileUtils;
import lombok.extern.log4j.Log4j2;

/**
 * 
 * This class is used to import video data from s3 upload storage into a given process. The upload is considered as complete if the process contains
 * either a jpg + mpg or a jpg + mp4 + mxf file. If the upload was completed, the current open step gets closed.
 * 
 */

@Log4j2
public class ImportAudioDataHandler implements TicketHandler<PluginReturnValue> {

    @Override
    public PluginReturnValue call(TaskTicket ticket) {
        String bucket = ticket.getProperties().get("bucket");

        String s3Key = ticket.getProperties().get("s3Key");
        Path destinationFolder
        = Paths.get(ticket.getProperties().get("destination"));


        log.debug("copy {} to {}", bucket + "/" + s3Key, destinationFolder);

        AmazonS3 s3 = S3FileUtils.createS3Client();
        TransferManager transferManager =
                TransferManagerBuilder.standard().withS3Client(s3).withMultipartUploadThreshold((long) (1 * 1024 * 1024 * 1024)).build();
        int index = s3Key.lastIndexOf('/');
        Path destinationFile;
        if (index != -1) {
            destinationFile = destinationFolder.resolve(s3Key.substring(index + 1));
        } else {
            destinationFile = destinationFolder.resolve(s3Key);
        }



        Copy copy = transferManager.copy(bucket, s3Key, ConfigurationHelper.getInstance().getS3Bucket(), S3FileUtils.path2Key(destinationFile));
        try {
            copy.waitForCompletion();
        } catch (AmazonClientException | InterruptedException e) {
            log.error(e);
        }
        transferManager.shutdownNow();
        String deleteFiles = ticket.getProperties().get("deleteFiles");
        if (StringUtils.isNotBlank(deleteFiles) && deleteFiles.equalsIgnoreCase("true")) {
            s3.deleteObject(bucket, s3Key);
            log.info("deleted file from bucket");
        }

        //delete everything under parent prefix
        if (StringUtils.isNotBlank(deleteFiles) && deleteFiles.equalsIgnoreCase("true")) {
            String prefix = s3Key.substring(0, s3Key.lastIndexOf('/'));
            ObjectListing listing = s3.listObjects(bucket, prefix);
            for (S3ObjectSummary os : listing.getObjectSummaries()) {
                s3.deleteObject(bucket, os.getKey());
            }
        }

        return PluginReturnValue.FINISH;
    }

    @Override
    public String getTicketHandlerName() {
        return "importVideoData";
    }

}
