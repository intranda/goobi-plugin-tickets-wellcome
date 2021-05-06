package org.goobi.api.mq;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.jms.JMSException;

import org.apache.commons.lang.StringUtils;
import org.goobi.production.enums.PluginReturnValue;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.transfer.Download;

import de.sub.goobi.helper.S3FileUtils;
import de.sub.goobi.helper.StorageProvider;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class DownloadS3Handler implements TicketHandler<PluginReturnValue> {

    @Override
    public String getTicketHandlerName() {
        return "downloads3";
    }

    /**
     * This class is used to download a file from an s3 bucket and store it in a local directory.
     * 
     * In case it is a zip file, a new ticket is created to extract the file after download.
     * 
     */

    @Override
    public PluginReturnValue call(TaskTicket ticket) {
        String bucket = ticket.getProperties().get("bucket");

        String s3Key = ticket.getProperties().get("s3Key");

        Path targetDir = Paths.get(ticket.getProperties().get("targetDir"));

        log.debug("download " + s3Key + " to " + targetDir);
        try {
            StorageProvider.getInstance().createDirectories(targetDir);
        } catch (IOException e1) {
            log.error("Unable to create temporary directory", e1);
            return PluginReturnValue.ERROR;
        }

        int index = s3Key.lastIndexOf('/');
        Path targetPath;
        if (index != -1) {
            targetPath = targetDir.resolve(s3Key.substring(index + 1));
        } else {
            targetPath = targetDir.resolve(s3Key);
        }
        S3FileUtils utils = (S3FileUtils) StorageProvider.getInstance();
        Download download = utils.getTransferManager().download(bucket, s3Key, targetPath.toFile());
        try {
            download.waitForCompletion();
        } catch (AmazonClientException | InterruptedException e) {
            log.error(e);
            // TODO cleanup
            return PluginReturnValue.ERROR;
        }
        log.info("saved file");
        String deleteFiles = ticket.getProperties().get("deleteFiles");
        if (StringUtils.isNotBlank(deleteFiles) && deleteFiles.equalsIgnoreCase("true")) {
            utils.getS3().deleteObject(bucket, s3Key);
            log.info("deleted file from bucket");
        }
        // check if it is an EP import or a regular one
        if (ticket.getProcessId() == null) {
            log.info("create EP import ticket");
            TaskTicket importEPTicket = TicketGenerator.generateSimpleTicket("importEP");
            importEPTicket.setProperties(ticket.getProperties());
            importEPTicket.getProperties().put("filename", targetPath.toString());
            try {
                TicketGenerator.submitInternalTicket(importEPTicket, QueueType.SLOW_QUEUE, "EP_import", 0);
            } catch (JMSException e) {
                log.error(e);
            }
        }

        // create a new ticket to extract data
        else if (targetPath.getFileName().toString().endsWith(".zip")) {
            log.info("create unzip ticket");
            TaskTicket unzipTticket = TicketGenerator.generateSimpleTicket("unzip");
            unzipTticket.setProcessId(ticket.getProcessId());
            unzipTticket.setProcessName(ticket.getProcessName());
            unzipTticket.setProperties(ticket.getProperties());
            unzipTticket.setStepId(ticket.getStepId());
            unzipTticket.setStepName(ticket.getStepName());
            unzipTticket.getProperties().put("filename", targetPath.toString());
            unzipTticket.getProperties().put("closeStep", "true");
            try {
                TicketGenerator.submitInternalTicket(unzipTticket, QueueType.SLOW_QUEUE, "unzip", ticket.getProcessId());
            } catch (JMSException e) {
                log.error(e);
            }

        }
        log.info("finished download ticket");
        return PluginReturnValue.FINISH;
    }

}
