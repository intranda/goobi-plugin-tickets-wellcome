package org.goobi.api.mq;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.goobi.beans.Process;
import org.goobi.beans.Processproperty;
import org.goobi.beans.Step;
import org.goobi.production.enums.LogType;
import org.goobi.production.enums.PluginReturnValue;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.TransferManager;

import de.sub.goobi.config.ConfigurationHelper;
import de.sub.goobi.helper.Helper;
import de.sub.goobi.helper.S3FileUtils;
import de.sub.goobi.helper.StorageProvider;
import de.sub.goobi.persistence.managers.ProcessManager;
import de.sub.goobi.persistence.managers.PropertyManager;
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
        Path destinationFolder = Paths.get(ticket.getProperties().get("destination"));

        S3FileUtils utils = (S3FileUtils) StorageProvider.getInstance();
        AmazonS3 s3 = utils.getS3();

        Process process = ProcessManager.getProcessById(ticket.getProcessId());

        // check process status
        boolean uploadIsAllowed = false;
        Step currentStep =  process.getAktuellerSchritt();
        if (currentStep != null) {
            // no open step found, abort

            switch (currentStep.getTitel()) {
                case "Bibliographic import":
                case "Video data import":
                case "Audio (Video) data import":
                case "Audio data import":
                case "Document import":
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
            Helper.addMessageToProcessJournal(ticket.getProcessId(), LogType.ERROR, "File import aborted, process has not the correct status.", "ticket");

            s3.deleteObject(bucket, s3Key);
            log.info("deleted file {} from bucket", s3Key);
            return PluginReturnValue.ERROR;
        }


        TransferManager transferManager = utils.getTransferManager();
        log.debug("copy {} to {}", bucket + "/" + s3Key, destinationFolder);
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

        List<Processproperty> properties = PropertyManager.getProcessPropertiesForProcess(process.getId());
        if (!properties.stream().anyMatch(pp -> pp.getTitel().equals("s3_import_bucket"))) {
            addProcesspropertyToProcess(process, "s3_import_bucket", bucket);
        }
        if (!properties.stream().anyMatch(pp -> pp.getTitel().equals("s3_import_prefix"))) {
            String prefix = s3Key.substring(0, s3Key.lastIndexOf('/'));
            addProcesspropertyToProcess(process, "s3_import_prefix", prefix);
        }


        String deleteFiles = ticket.getProperties().get("deleteFiles");
        if (StringUtils.isNotBlank(deleteFiles) && deleteFiles.equalsIgnoreCase("true")) {
            s3.deleteObject(bucket, s3Key);
            log.info("deleted file {} from bucket", s3Key);
        }

        return PluginReturnValue.FINISH;
    }

    private void addProcesspropertyToProcess(Process process, String name, String value) {
        Processproperty pp = new Processproperty();
        pp.setProzess(process);
        pp.setTitel(name);
        pp.setWert(value);

        PropertyManager.saveProcessProperty(pp);
    }


    @Override
    public String getTicketHandlerName() {
        return "importAudioData";
    }

}
