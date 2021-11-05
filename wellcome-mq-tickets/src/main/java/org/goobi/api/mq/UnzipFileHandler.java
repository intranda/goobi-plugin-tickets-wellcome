package org.goobi.api.mq;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.goobi.beans.LogEntry;
import org.goobi.beans.Process;
import org.goobi.beans.Step;
import org.goobi.production.enums.LogType;
import org.goobi.production.enums.PluginReturnValue;

import de.sub.goobi.helper.CloseStepHelper;
import de.sub.goobi.helper.StorageProvider;
import de.sub.goobi.helper.enums.StepStatus;
import de.sub.goobi.persistence.managers.ProcessManager;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class UnzipFileHandler implements TicketHandler<PluginReturnValue> {

    @Override
    public String getTicketHandlerName() {
        return "unzip";
    }

    @Override
    public PluginReturnValue call(TaskTicket ticket) {

        String source = ticket.getProperties().get("filename");

        String tifFolder = ticket.getProperties().get("tifFolder");

        if (StringUtils.isBlank(tifFolder)) {
            tifFolder = ticket.getProperties().get("destination");
        }
        String jp2Folder = ticket.getProperties().get("jp2Folder");
        if (StringUtils.isBlank(jp2Folder)) {
            jp2Folder = tifFolder;
        }
        Path workDir = null;
        Path zipFile = null;
        try {
            workDir = Files.createTempDirectory(UUID.randomUUID().toString());
            zipFile = Paths.get(source);
            Path directory = unzip(zipFile, workDir);

            //            Files.delete(zipFile);
            // alto files are imported into alto directory
            List<Path> altoFiles = new ArrayList<>();
            // objects are imported into the master directory
            List<Path> objectFiles = new ArrayList<>();

            // check if the extracted file contains a sub folder
            //            try (DirectoryStream<Path> folderFiles = Files.newDirectoryStream(directory)) {
            //                for (Path file : folderFiles) {
            //                    if (Files.isDirectory(file) && !file.getFileName().toString().startsWith("__MAC")) {
            //                        directory = file;
            //                        break;
            //                    }
            //                }
            //            } catch (IOException e1) {
            //                log.error(e1);
            //            }

            try (DirectoryStream<Path> folderFiles = Files.newDirectoryStream(directory)) {
                for (Path file : folderFiles) {
                    if (Files.isDirectory(file) && !file.getFileName().toString().startsWith("__MAC")) {
                        // found unexpected data
                        LogEntry.build(ticket.getProcessId())
                        .withContent("File import aborted, found unexpected sub folder in zip file.")
                        .withType(LogType.INFO)
                        .persist();
                        FileUtils.deleteQuietly(zipFile.toFile());
                        FileUtils.deleteQuietly(workDir.toFile());
                        return PluginReturnValue.ERROR;
                    }
                    String fileName = file.getFileName().toString();
                    String fileNameLower = fileName.toLowerCase();
                    if (fileNameLower.endsWith(".xml") && !fileNameLower.startsWith(".")) {
                        altoFiles.add(file);
                    } else if (!fileNameLower.startsWith(".")) {
                        objectFiles.add(file);
                    }
                }
            } catch (IOException e1) {
                log.error(e1);
            }

            if (objectFiles.isEmpty()) {
                LogEntry.build(ticket.getProcessId()).withContent("File import aborted, found no files to import").withType(LogType.INFO).persist();
                FileUtils.deleteQuietly(zipFile.toFile());
                FileUtils.deleteQuietly(workDir.toFile());
                return PluginReturnValue.ERROR;
            }

            Path masterDir = Paths.get(tifFolder);
            Path derivativeDir = Paths.get(jp2Folder);

            if (!Files.exists(masterDir)) {
                Files.createDirectories(masterDir);
            }
            if (!Files.exists(derivativeDir)) {
                Files.createDirectories(derivativeDir);
            }

            // list all existing files
            List<String> files = StorageProvider.getInstance().list(masterDir.toString());
            files.addAll(StorageProvider.getInstance().list(derivativeDir.toString()));

            if (files.isEmpty()) {
                // import the data only if the process is empty
                for (Path object : objectFiles) {
                    if (object.getFileName().toString().toLowerCase().endsWith("jp2")) {
                        StorageProvider.getInstance().copyFile(object, derivativeDir.resolve(object.getFileName().toString().replace(" ", "")));
                    } else {
                        StorageProvider.getInstance().copyFile(object, masterDir.resolve(object.getFileName().toString().replace(" ", "")));
                    }
                }

                String closeStepValue = ticket.getProperties().get("closeStep");

                if (StringUtils.isNotBlank(closeStepValue) && "true".equals(closeStepValue)) {
                    Process process = ProcessManager.getProcessById(ticket.getProcessId());

                    Step stepToClose = null;

                    for (Step processStep : process.getSchritte()) {
                        if (processStep.getBearbeitungsstatusEnum() == StepStatus.OPEN
                                || processStep.getBearbeitungsstatusEnum() == StepStatus.INWORK) {
                            // check against a list of configured step names ?
                            stepToClose = processStep;
                            break;
                        }
                    }
                    if (stepToClose != null) {
                        CloseStepHelper.closeStep(stepToClose, null);
                    }
                }
            } else {
                LogEntry entry =
                        LogEntry.build(ticket.getProcessId()).withContent("File import aborted, directory is not empty").withType(LogType.INFO);
                entry.persist();
            }

            FileUtils.deleteQuietly(zipFile.toFile());
            FileUtils.deleteQuietly(workDir.toFile());

        } catch (IOException e) {
            log.error(e);
            LogEntry entry = LogEntry.build(ticket.getProcessId()).withContent(e.getMessage()).withType(LogType.ERROR);
            entry.persist();
            FileUtils.deleteQuietly(zipFile.toFile());
            FileUtils.deleteQuietly(workDir.toFile());
            return PluginReturnValue.ERROR;
        }

        return PluginReturnValue.FINISH;
    }

    public static Path unzip(final Path zipFile, final Path output) throws IOException {
        Path unzippedFolder = output;
        try (ZipInputStream zipInputStream = new ZipInputStream(Files.newInputStream(zipFile))) {
            ZipEntry entry;
            while ((entry = zipInputStream.getNextEntry()) != null) {
                final Path toPath = output.resolve(entry.getName());
                if (entry.isDirectory()) {
                    Files.createDirectories(toPath);
                } else {
                    Path directory = toPath.getParent();
                    if (!Files.exists(directory)) {
                        Files.createDirectories(directory);
                    }
                    Files.copy(zipInputStream, toPath);
                }
            }
        }
        // check if the extracted file contains a sub folder
        try (DirectoryStream<Path> folderFiles = Files.newDirectoryStream(output)) {
            for (Path file : folderFiles) {
                if (Files.isDirectory(file) && !file.getFileName().toString().startsWith("__MAC")) {
                    unzippedFolder = file;
                    break;
                }
            }
        } catch (IOException e1) {
            log.error(e1);
        }
        return unzippedFolder;
    }
}
