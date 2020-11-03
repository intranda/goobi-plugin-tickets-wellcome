package org.goobi.api.mq;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.lang.StringUtils;
import org.goobi.production.enums.PluginReturnValue;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.TransferManager;

import de.sub.goobi.config.ConfigurationHelper;
import de.sub.goobi.helper.S3FileUtils;
import de.sub.goobi.helper.StorageProvider;
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
        S3FileUtils utils = (S3FileUtils) StorageProvider.getInstance();
        AmazonS3 s3 = utils.getS3();
        TransferManager transferManager =utils.getTransferManager();
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
        String deleteFiles = ticket.getProperties().get("deleteFiles");
        if (StringUtils.isNotBlank(deleteFiles) && deleteFiles.equalsIgnoreCase("true")) {
            s3.deleteObject(bucket, s3Key);
            log.info("deleted file {} from bucket", s3Key);
        }


        return PluginReturnValue.FINISH;
    }

    @Override
    public String getTicketHandlerName() {
        return "importAudioData";
    }

}
