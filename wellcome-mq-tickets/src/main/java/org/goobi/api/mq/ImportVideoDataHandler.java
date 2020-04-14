package org.goobi.api.mq;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.goobi.beans.Process;
import org.goobi.beans.Step;
import org.goobi.production.enums.PluginReturnValue;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;

import de.sub.goobi.config.ConfigurationHelper;
import de.sub.goobi.helper.CloseStepHelper;
import de.sub.goobi.helper.S3FileUtils;
import de.sub.goobi.helper.StorageProvider;
import de.sub.goobi.helper.enums.StepStatus;
import de.sub.goobi.persistence.managers.ProcessManager;
import lombok.extern.log4j.Log4j2;

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

        AmazonS3 s3 = S3FileUtils.createS3Client();

        int index = s3Key.lastIndexOf('/');
        Path destinationFile;
        if (index != -1) {
            destinationFile = destinationFolder.resolve(s3Key.substring(index + 1));
        } else {
            destinationFile = destinationFolder.resolve(s3Key);
        }

        TransferManager tm = TransferManagerBuilder.standard()
                .withS3Client(s3)
                .withMultipartUploadThreshold((long) (1 * 1024 * 1024 * 1024))
                .build();

        Copy copy = tm.copy(bucket, s3Key, ConfigurationHelper.getInstance().getS3Bucket(), S3FileUtils.path2Key(destinationFile));
        try {
            copy.waitForCompletion();
        } catch (AmazonClientException | InterruptedException e) {
            log.error(e);
        }

        // check if the upload is complete
        List<String> filenamesInFolder = StorageProvider.getInstance().list(destinationFolder.toString());
        boolean posterFound = false;
        boolean mpegFound = false;
        boolean mp4Found = false;
        boolean mxfFound = false;

        for (String filename : filenamesInFolder) {
            String suffix = filename.substring(filename.indexOf(".") + 1);
            switch (suffix) {

                case "jpg":
                case "JPG":
                case "jpeg":
                case "JPEG":
                    posterFound = true;
                    break;
                case "mpg":
                case "MPG":
                case "mpeg":
                case "MPEG":
                    mpegFound = true;
                    break;
                case "mp4":
                case "MP4":
                    mp4Found = true;
                    break;
                case "mxf":
                case "MXF":
                    mxfFound = true;
                    break;
            }
        }

        String deleteFiles = ticket.getProperties().get("deleteFiles");
        if (StringUtils.isNotBlank(deleteFiles) && deleteFiles.equalsIgnoreCase("true")) {
            s3.deleteObject(bucket, s3Key);
            log.info("deleted file from bucket");
        }

        // upload is complete, if poster + mpg or poster + mp4 + mxf are available
        if ((posterFound && mpegFound) || (posterFound && mp4Found && mxfFound)) {
            // close current task
            Process process = ProcessManager.getProcessById(ticket.getProcessId());
            Step stepToClose = null;

            for (Step processStep : process.getSchritte()) {
                if (processStep.getBearbeitungsstatusEnum() == StepStatus.OPEN || processStep.getBearbeitungsstatusEnum() == StepStatus.INWORK) {
                    stepToClose = processStep;
                    break;
                }
            }
            if (stepToClose != null) {
                CloseStepHelper.closeStep(stepToClose, null);
            }

            //delete everything under parent prefix
            if (StringUtils.isNotBlank(deleteFiles) && deleteFiles.equalsIgnoreCase("true")) {
                String prefix = s3Key.substring(0, s3Key.lastIndexOf('/'));
                ObjectListing listing = s3.listObjects(bucket, prefix);
                for (S3ObjectSummary os : listing.getObjectSummaries()) {
                    s3.deleteObject(bucket, os.getKey());
                }
            }
        }

        return PluginReturnValue.FINISH;
    }

    @Override
    public String getTicketHandlerName() {
        return "importVideoData";
    }

}
