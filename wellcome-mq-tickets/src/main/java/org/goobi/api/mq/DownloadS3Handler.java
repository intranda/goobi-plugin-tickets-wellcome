package org.goobi.api.mq;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.jms.JMSException;

import org.apache.commons.lang.StringUtils;
import org.goobi.production.enums.PluginReturnValue;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;

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

        AmazonS3 s3 = S3FileUtils.createS3Client();

        int index = s3Key.lastIndexOf('/');
        Path targetPath;
        if (index != -1) {
            targetPath = targetDir.resolve(s3Key.substring(index + 1));
        } else {
            targetPath = targetDir.resolve(s3Key);
        }

        try (S3Object obj = s3.getObject(bucket, s3Key); InputStream in = obj.getObjectContent()) {
            Files.copy(in, targetPath);
        } catch (IOException e) {
            log.error(e);
            return PluginReturnValue.ERROR;
        }
        log.info("saved file");
        String deleteFiles = ticket.getProperties().get("deleteFiles");
        if (StringUtils.isNotBlank(deleteFiles) && deleteFiles.equalsIgnoreCase("true")) {
            s3.deleteObject(bucket, s3Key);
            log.info("deleted file from bucket");
        }

        // check if it is an EP import or a regular one
        if (ticket.getProcessId() == null) {
            log.info("create EP import ticket");
            TaskTicket importEPTicket = TicketGenerator.generateSimpleTicket("importEP");
            importEPTicket.setProperties(ticket.getProperties());
            importEPTicket.getProperties().put("filename", targetPath.toString());
            try {
                TicketGenerator.submitTicket(importEPTicket, QueueType.SLOW_QUEUE);
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
                TicketGenerator.submitTicket(unzipTticket, QueueType.SLOW_QUEUE);
            } catch (JMSException e) {
                log.error(e);
            }

        }
        log.info("finished download ticket");
        return PluginReturnValue.FINISH;
    }

}
