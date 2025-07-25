package org.goobi.api.mq;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;
import org.goobi.beans.Process;
import org.goobi.beans.Processproperty;
import org.goobi.beans.Step;
import org.goobi.managedbeans.LoginBean;
import org.goobi.production.enums.PluginReturnValue;
import org.goobi.production.flow.jobs.HistoryAnalyserJob;

import de.sub.goobi.config.ConfigurationHelper;
import de.sub.goobi.helper.BeanHelper;
import de.sub.goobi.helper.Helper;
import de.sub.goobi.helper.S3FileUtils;
import de.sub.goobi.helper.ScriptThreadWithoutHibernate;
import de.sub.goobi.helper.StorageProvider;
import de.sub.goobi.helper.enums.StepEditType;
import de.sub.goobi.helper.enums.StepStatus;
import de.sub.goobi.helper.exceptions.DAOException;
import de.sub.goobi.helper.exceptions.SwapException;
import de.sub.goobi.persistence.managers.ProcessManager;
import de.sub.goobi.persistence.managers.PropertyManager;
import de.sub.goobi.persistence.managers.StepManager;
import lombok.extern.log4j.Log4j2;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import ugh.dl.ContentFile;
import ugh.dl.DigitalDocument;
import ugh.dl.DocStruct;
import ugh.dl.Fileformat;
import ugh.dl.Metadata;
import ugh.dl.MetadataType;
import ugh.dl.Person;
import ugh.dl.Prefs;
import ugh.exceptions.MetadataTypeNotAllowedException;
import ugh.exceptions.PreferencesException;
import ugh.exceptions.ReadException;
import ugh.exceptions.TypeNotAllowedAsChildException;
import ugh.exceptions.TypeNotAllowedForParentException;
import ugh.exceptions.WriteException;
import ugh.fileformats.mets.MetsMods;

@Log4j2
public class ImportEPHandler implements TicketHandler<PluginReturnValue> {

    @Override
    public String getTicketHandlerName() {
        return "importEP";
    }

    @Override
    public PluginReturnValue call(TaskTicket ticket) {

        log.info("start EP import");

        Process templateUpdate = ProcessManager.getProcessById(Integer.parseInt(ticket.getProperties().get("updateTemplateId")));
        Process templateNew = ProcessManager.getProcessById(Integer.parseInt(ticket.getProperties().get("templateId")));

        Prefs prefs = templateNew.getRegelsatz().getPreferences();

        List<Path> tifFiles = new ArrayList<>();
        Path zipfFile = Paths.get(ticket.getProperties().get("filename"));
        Path workDir = null;
        Path directory = null;
        try {
            workDir = Files.createTempDirectory(UUID.randomUUID().toString());
            directory = UnzipFileHandler.unzip(zipfFile, workDir);

        } catch (IOException e2) {
            log.error(e2);
            FileUtils.deleteQuietly(zipfFile.toFile());
            FileUtils.deleteQuietly(workDir.toFile());
            // move zip file to failed bucket
            moveZipToFailed(ticket);
            return PluginReturnValue.ERROR;
        }

        log.info("use template " + templateNew.getId());

        Path csvFile = null;
        // check if the extracted file contains a sub folder

        try (DirectoryStream<Path> folderFiles = Files.newDirectoryStream(directory)) {
            for (Path file : folderFiles) {
                String fileName = file.getFileName().toString();
                log.info("found " + fileName);
                String fileNameLower = fileName.toLowerCase();
                if (fileNameLower.endsWith(".csv") && !fileNameLower.startsWith(".")) {
                    csvFile = file;
                    log.info("set csv file to " + fileName);
                }
                if ((fileNameLower.endsWith(".tif") || fileNameLower.endsWith(".tiff") || fileNameLower.endsWith(".mp4"))
                        && !fileNameLower.startsWith(".")) {
                    tifFiles.add(file);
                }
            }
        } catch (IOException e1) {
            log.error(e1);
            FileUtils.deleteQuietly(zipfFile.toFile());
            FileUtils.deleteQuietly(workDir.toFile());
            moveZipToFailed(ticket);
            return PluginReturnValue.ERROR;
        }

        Collections.sort(tifFiles);
        try {
            boolean wcp = createProcess(csvFile, tifFiles, prefs, templateNew, templateUpdate);
            if (!wcp) {
                FileUtils.deleteQuietly(zipfFile.toFile());
                FileUtils.deleteQuietly(workDir.toFile());
                moveZipToFailed(ticket);
                return PluginReturnValue.ERROR;

            } else {
                // process created. Now delete this folder.
                FileUtils.deleteQuietly(zipfFile.toFile());
                FileUtils.deleteQuietly(workDir.toFile());
                S3FileUtils utils = (S3FileUtils) StorageProvider.getInstance();
                deleteObject(utils.getS3(), ticket.getProperties().get("bucket"), ticket.getProperties().get("s3Key"));
                return PluginReturnValue.FINISH;
            }
        } catch (FileNotFoundException e) {
            log.error("Cannot import csv file: " + csvFile + "\n", e);
            FileUtils.deleteQuietly(zipfFile.toFile());
            FileUtils.deleteQuietly(workDir.toFile());
            moveZipToFailed(ticket);
            return PluginReturnValue.ERROR;
        } catch (PreferencesException | WriteException | ReadException | IOException | InterruptedException | SwapException | DAOException e) {
            log.error("Unable to create Goobi Process\n", e);
            FileUtils.deleteQuietly(zipfFile.toFile());
            FileUtils.deleteQuietly(workDir.toFile());
            moveZipToFailed(ticket);
            return PluginReturnValue.ERROR;
        }
    }

    private void moveZipToFailed(TaskTicket ticket) {
        S3FileUtils utils = (S3FileUtils) StorageProvider.getInstance();
        String bucket = ticket.getProperties().get("bucket");
        String key = ticket.getProperties().get("s3Key");
        log.debug(ticket.getProperties());
        log.debug("Copying from {}/{} to {}/{}", bucket, key, bucket, "failed/" + key.substring(key.lastIndexOf('/') + 1));

        CopyObjectRequest copyReq = CopyObjectRequest.builder()
                .sourceBucket(bucket)
                .sourceKey(key)
                .destinationBucket(bucket)
                .destinationKey("failed/" + key.substring(key.lastIndexOf('/') + 1))
                .build();

        CompletableFuture<CopyObjectResponse> copyRes = utils.getS3().copyObject(copyReq);
        copyRes.join();

        deleteObject(utils.getS3(), ticket.getProperties().get("bucket"), ticket.getProperties().get("s3Key"));
    }

    private void deleteObject(S3AsyncClient s3, String bucket, String key) {
        ArrayList<ObjectIdentifier> toDelete = new ArrayList<>();
        toDelete.add(ObjectIdentifier.builder()
                .key(key)
                .build());

        DeleteObjectsRequest dor = DeleteObjectsRequest.builder()
                .bucket(bucket)
                .delete(Delete.builder()
                        .objects(toDelete)
                        .build())
                .build();

        s3.deleteObjects(dor);
    }

    private boolean createProcess(Path csvFile, List<Path> tifFiles, Prefs prefs, Process templateNew, Process templateUpdate)
            throws FileNotFoundException, IOException, InterruptedException, SwapException, DAOException, PreferencesException, WriteException,
            ReadException {
        log.info("read csv file " + csvFile.getFileName().toString());
        if (!Files.exists(csvFile)) {
            throw new FileNotFoundException();
        }

        Map<String, Integer> indexMap = new HashMap<>();
        List<String[]> values = new ArrayList<>();
        readFile(csvFile, indexMap, values);

        String referenceNumber = getValue("Reference", 0, indexMap, values);
        List<Path> newTifFiles = new ArrayList<>();
        int count = 1;
        log.info("read tif files");
        for (Path tifFile : tifFiles) {
            String fileName = tifFile.getFileName().toString();
            String ext = fileName.substring(fileName.lastIndexOf('.')).toLowerCase();
            String newFileName = fileName;
            // only rename the EP shoot names
            if (referenceNumber.startsWith("EP")) {
                newFileName = referenceNumber.replaceAll(" |\t", "_") + String.format("_%03d", count) + ext;
            }
            newTifFiles.add(tifFile.getParent().resolve(newFileName));
            count++;
        }
        log.info("create metadata file");
        Fileformat ff = convertData(indexMap, values, newTifFiles, prefs);

        if (ff == null) {
            return false;
        }

        Process process = null;

        boolean existsInGoobiNotDone = false;
        List<Process> processes = ProcessManager.getProcesses("", "prozesse.titel='" + referenceNumber.replaceAll(" |\t", "_") + "'", null);
        log.debug("found " + processes.size() + " processes with title " + referenceNumber.replaceAll(" |\t", "_"));
        for (Process p : processes) {
            if (!"100000000".equals(p.getSortHelperStatus())) {
                existsInGoobiNotDone = true;
                break;
            }
        }

        if (existsInGoobiNotDone) {
            // does exist in Goobi, but is not done => wait (return error)
            log.warn(String.format(
                    "Editorial ingest: A process with identifier %s already exists in a non-finished state. Will not create a new process.",
                    referenceNumber.replaceAll(" |\t", "_")));
            return false;
        }
        boolean existsOnS3 = checkIfExistsOnS3(referenceNumber);

        if (existsOnS3) {
            log.info("create update process");
            // is already on s3, but everything in Goobi went through => update
            process = cloneTemplate(templateUpdate);
        } else {
            log.info("create new process");
            // not in Goobi and not on s3 => new shoot
            process = cloneTemplate(templateNew);
        }

        // set title
        process.setTitel(referenceNumber.replaceAll(" |\t", "_"));

        NeuenProzessAnlegen(process, templateNew, ff, prefs);
        log.info("saved process " + process.getTitel());
        saveProperty(process, "b-number", referenceNumber);
        saveProperty(process, "CollectionName1", "Editorial Photography");
        saveProperty(process, "CollectionName2", referenceNumber);
        saveProperty(process, "securityTag", "open");
        saveProperty(process, "schemaName", "Millennium");
        saveProperty(process, "archiveStatus", referenceNumber.startsWith("CP") ? "archived" : "contemporary");

        saveProperty(process, "Keywords", getValue("People", indexMap, values) + ", " + getValue("Keywords", indexMap, values));
        String creators = "";
        String staff = getValue("Staff Photog", indexMap, values);
        String freelance = getValue("Freelance Photog", indexMap, values);
        if (StringUtils.isBlank(freelance)) {
            freelance = getValue("Freelancer", indexMap, values);
        }
        if (staff != null && !staff.isEmpty()) {
            creators = staff;
            if (freelance != null && !freelance.isEmpty()) {
                creators += "/" + freelance;
            }
        } else if (!freelance.isEmpty()) {
            creators = freelance;
        }
        saveProperty(process, "Creators", creators);
        log.info("saved properties");
        // copy the files
        Path processDir = Paths.get(process.getProcessDataDirectory());
        Path importDir = processDir.resolve("import");
        Files.createDirectories(importDir);
        log.trace(String.format("Copying %s to %s (size: %d)", csvFile.toAbsolutePath().toString(),
                importDir.resolve(csvFile.getFileName()).toString(), Files.size(csvFile)));
        StorageProvider.getInstance().copyFile(csvFile, importDir.resolve(csvFile.getFileName()));

        Path imagesDir = Paths.get(process.getImagesOrigDirectory(false));
        count = 0;
        for (Path tifFile : tifFiles) {
            String newFileName = newTifFiles.get(count).getFileName().toString();
            log.trace(String.format("Copying %s to %s (size: %d)", tifFile.toAbsolutePath().toString(), imagesDir.resolve(newFileName).toString(),
                    Files.size(tifFile)));
            StorageProvider.getInstance().copyFile(tifFile, imagesDir.resolve(newFileName));
            count++;
        }
        log.info("copied data to process");
        // start work for process
        List<Step> steps = StepManager.getStepsForProcess(process.getId());
        for (Step s : steps) {
            if (StepStatus.OPEN.equals(s.getBearbeitungsstatusEnum()) && s.isTypAutomatisch()) {
                ScriptThreadWithoutHibernate myThread = new ScriptThreadWithoutHibernate(s);
                myThread.start();
            }
        }
        return true;
    }

    private void readFile(Path csvFile, Map<String, Integer> indexMap, List<String[]> values) throws FileNotFoundException, IOException {
        boolean firstLine = true;

        try (Reader r = new FileReader(csvFile.toFile())) {
            Iterable<CSVRecord> records = CSVFormat.RFC4180.parse(r);
            for (CSVRecord record : records) {
                if (firstLine) {
                    readIndex(record, indexMap, values);
                    firstLine = false;
                } else {
                    readLine(record, indexMap, values);
                }
            }
        }
    }

    private void readIndex(CSVRecord record, Map<String, Integer> indexMap, List<String[]> values) {
        int idx = 0;
        for (String element : record) {
            indexMap.put(element, idx);
            idx++;
        }
    }

    private void readLine(CSVRecord record, Map<String, Integer> indexMap, List<String[]> values) {
        int idx = 0;
        String[] lineValues = new String[indexMap.size()];
        for (String element : record) {
            lineValues[idx] = element;
            idx++;
        }
        values.add(lineValues);
    }

    private String getValue(String name, int row, Map<String, Integer> indexMap, List<String[]> values) {
        Integer index = indexMap.get(name);
        if (index == null) {
            return null;
        }
        return values.get(row)[index];
    }

    private String getValue(String name, Map<String, Integer> indexMap, List<String[]> values) {
        String value = this.getValue(name, 0, indexMap, values);
        if (value == null) {
            return "";
        }
        return value;
    }

    private boolean checkIfExistsOnS3(final String _reference) {
        if (ConfigurationHelper.getInstance().useCustomS3()) {
            return false;
        }
        String bucket;
        try {
            XMLConfiguration config = new XMLConfiguration("/opt/digiverso/goobi/config/plugin_wellcome_editorial_process_creation.xml");
            bucket = config.getString("bucket", "wellcomecollection-editorial-photography");// "wellcomecollection-editorial-photography";
            log.debug("using bucket " + bucket);
        } catch (ConfigurationException e) {
            bucket = "wellcomecollection-editorial-photography";
            log.debug("using bucket " + bucket);
        }
        String reference = _reference.replaceAll(" |\t", "_");
        int refLen = reference.length();
        String keyPrefix = reference.substring(refLen - 2, refLen) + "/" + reference + "/";
        String key = keyPrefix + reference + ".xml";
        S3FileUtils utils = (S3FileUtils) StorageProvider.getInstance();

        ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
                .bucket(bucket)
                .delimiter("/")
                .prefix(key);

        CompletableFuture<ListObjectsV2Response> response = utils.getS3().listObjectsV2(requestBuilder.build());
        ListObjectsV2Response resp = response.toCompletableFuture().join();

        return !resp.contents().isEmpty();
    }

    private Process cloneTemplate(Process template) {
        Process process = new Process();

        process.setIstTemplate(false);
        process.setInAuswahllisteAnzeigen(false);
        process.setProjekt(template.getProjekt());
        process.setRegelsatz(template.getRegelsatz());
        process.setDocket(template.getDocket());

        BeanHelper bHelper = new BeanHelper();
        bHelper.SchritteKopieren(template, process);
        bHelper.EigenschaftenKopieren(template, process);

        return process;
    }

    private void saveProperty(Process process, String name, String value) {
        Processproperty pe = new Processproperty();
        pe.setTitel(name);
        pe.setWert(value);
        pe.setProzess(process);
        PropertyManager.saveProcessProperty(pe);
    }

    private void NeuenProzessAnlegen(Process process, Process template, Fileformat ff, Prefs prefs)
            throws DAOException, PreferencesException, IOException, InterruptedException, SwapException, WriteException, ReadException {

        for (Step step : process.getSchritteList()) {

            step.setBearbeitungszeitpunkt(process.getErstellungsdatum());
            step.setEditTypeEnum(StepEditType.AUTOMATIC);
            LoginBean loginForm = Helper.getLoginBean();
            if (loginForm != null) {
                step.setBearbeitungsbenutzer(loginForm.getMyBenutzer());
            }

            if (step.getBearbeitungsstatusEnum() == StepStatus.DONE) {
                step.setBearbeitungsbeginn(process.getErstellungsdatum());

                Date myDate = new Date();
                step.setBearbeitungszeitpunkt(myDate);
                step.setBearbeitungsende(myDate);
            }

        }

        ProcessManager.saveProcess(process);

        /*
         * -------------------------------- Imagepfad hinzufügen (evtl. vorhandene
         * zunächst löschen) --------------------------------
         */
        try {
            MetadataType mdt = prefs.getMetadataTypeByName("pathimagefiles");
            List<? extends Metadata> alleImagepfade = ff.getDigitalDocument().getPhysicalDocStruct().getAllMetadataByType(mdt);
            if (alleImagepfade != null && !alleImagepfade.isEmpty()) {
                for (Metadata md : alleImagepfade) {
                    ff.getDigitalDocument().getPhysicalDocStruct().getAllMetadata().remove(md);
                }
            }
            Metadata newmd = new Metadata(mdt);
            if (SystemUtils.IS_OS_WINDOWS) {
                newmd.setValue("file:/" + process.getImagesDirectory() + process.getTitel().trim() + "_tif");
            } else {
                newmd.setValue("file://" + process.getImagesDirectory() + process.getTitel().trim() + "_tif");
            }
            ff.getDigitalDocument().getPhysicalDocStruct().addMetadata(newmd);

            /* Rdf-File schreiben */
            process.writeMetadataFile(ff);

        } catch (ugh.exceptions.DocStructHasNoTypeException | MetadataTypeNotAllowedException e) {
            log.error(e);
        }

        // Adding process to history
        HistoryAnalyserJob.updateHistoryForProzess(process);

        ProcessManager.saveProcess(process);

        process.readMetadataFile();

    }

    private Fileformat convertData(Map<String, Integer> indexMap, List<String[]> values, List<Path> tifFiles, Prefs prefs) {
        Fileformat ff = null;
        try {

            ff = new MetsMods(prefs);
            DigitalDocument dd = new DigitalDocument();
            ff.setDigitalDocument(dd);

            // Determine the root docstruct type
            String dsType = "EditorialPhotography";

            DocStruct dsRoot = dd.createDocStruct(prefs.getDocStrctTypeByName(dsType));

            Metadata md = new Metadata(prefs.getMetadataTypeByName("TitleDocMain"));
            String title = getValue("Title", indexMap, values);
            if (title.isEmpty()) {
                return null;
            }
            md.setValue(title);
            dsRoot.addMetadata(md);
            md = new Metadata(prefs.getMetadataTypeByName("ShootType"));
            String shootType = getValue("Shoot Type", indexMap, values);
            if (shootType.isEmpty()) {
                return null;
            }
            md.setValue(shootType);
            dsRoot.addMetadata(md);
            md = new Metadata(prefs.getMetadataTypeByName("CatalogIDDigital"));
            String reference = getValue("Reference", indexMap, values).replaceAll(" |\t", "_");
            if (reference.isEmpty()) {
                return null;
            }
            md.setValue(reference);
            dsRoot.addMetadata(md);
            md = new Metadata(prefs.getMetadataTypeByName("PlaceOfPublication"));
            md.setValue(getValue("Location", indexMap, values));
            dsRoot.addMetadata(md);
            md = new Metadata(prefs.getMetadataTypeByName("Contains"));
            md.setValue(getValue("Caption", indexMap, values));
            dsRoot.addMetadata(md);
            md = new Metadata(prefs.getMetadataTypeByName("People"));
            md.setValue(getValue("People", indexMap, values));
            dsRoot.addMetadata(md);
            md = new Metadata(prefs.getMetadataTypeByName("Description"));
            md.setValue(getValue("Keywords", indexMap, values));
            dsRoot.addMetadata(md);
            md = new Metadata(prefs.getMetadataTypeByName("Usage"));
            md.setValue(getValue("Intended Usage", indexMap, values));
            dsRoot.addMetadata(md);
            md = new Metadata(prefs.getMetadataTypeByName("AccessLicense"));
            md.setValue(getValue("Usage Terms", indexMap, values));
            dsRoot.addMetadata(md);

            String name = getValue("Staff Photog", indexMap, values);
            if (!StringUtils.isBlank(name)) {
                Person p = new Person(prefs.getMetadataTypeByName("Photographer"));
                int lastSpace = name.lastIndexOf(' ');
                String firstName = name.substring(0, lastSpace);
                String lastName = name.substring(lastSpace + 1, name.length());
                p.setFirstname(firstName);
                p.setLastname(lastName);
                dsRoot.addPerson(p);
            }

            name = getValue("Freelance Photog", indexMap, values);
            if (StringUtils.isNotBlank(name)) {
                name = getValue("Freelancer", indexMap, values);
            }
            if (!StringUtils.isBlank(name)) {
                Person p = new Person(prefs.getMetadataTypeByName("Creator"));
                int lastSpace = name.lastIndexOf(' ');
                String firstName = name.substring(0, lastSpace);
                String lastName = name.substring(lastSpace + 1, name.length());
                p.setFirstname(firstName);
                p.setLastname(lastName);
                dsRoot.addPerson(p);
            }

            dd.setLogicalDocStruct(dsRoot);

            DocStruct dsBoundBook = dd.createDocStruct(prefs.getDocStrctTypeByName("BoundBook"));
            // TODO add files to dsBoundBook (correctly)
            int pageNo = 0;
            for (Path tifPath : tifFiles) {
                DocStruct page = dd.createDocStruct(prefs.getDocStrctTypeByName("page"));
                try {
                    // physical page no
                    dsBoundBook.addChild(page);
                    MetadataType mdt = prefs.getMetadataTypeByName("physPageNumber");
                    Metadata mdTemp = new Metadata(mdt);
                    mdTemp.setValue(String.valueOf(pageNo));
                    page.addMetadata(mdTemp);

                    // logical page no
                    mdt = prefs.getMetadataTypeByName("logicalPageNumber");
                    mdTemp = new Metadata(mdt);

                    mdTemp.setValue("uncounted");

                    page.addMetadata(mdTemp);
                    ContentFile cf = new ContentFile();

                    cf.setLocation("file://" + tifPath.toAbsolutePath().toString());

                    page.addContentFile(cf);

                } catch (TypeNotAllowedAsChildException | MetadataTypeNotAllowedException e) {
                    log.error(e);
                }
                pageNo++;
            }

            dd.setPhysicalDocStruct(dsBoundBook);

            // Collect MODS metadata

            // Add dummy volume to anchors ??
            // generateDefaultValues(prefs, collectionName, dsRoot, dsBoundBook);

        } catch (PreferencesException | TypeNotAllowedForParentException | MetadataTypeNotAllowedException e) {
            log.error(e);
        }
        return ff;
    }

}
