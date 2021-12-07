package eu.europeana.cloud.service.mcs.persistent;

import com.google.common.base.Throwables;
import eu.europeana.cloud.common.model.*;
import eu.europeana.cloud.common.response.RepresentationRevisionResponse;
import eu.europeana.cloud.common.utils.FileUtils;
import eu.europeana.cloud.common.utils.RevisionUtils;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.exception.*;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraDataSetDAO;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraRecordDAO;
import eu.europeana.cloud.service.mcs.persistent.exception.SystemException;
import eu.europeana.cloud.service.mcs.persistent.swift.PutResult;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.function.Consumer;

/**
 * Implementation of record service using Cassandra as storage.
 */
@Service
public class CassandraRecordService implements RecordService {

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraRecordService.class);

    @Autowired
    private CassandraRecordDAO recordDAO;

    @Autowired
    private DataSetService dataSetService;

    @Autowired
    private CassandraDataSetDAO dataSetDAO;

    @Autowired
    private DynamicContentProxy contentDAO;

    @Autowired
    private UISClientHandler uis;


    /**
     * @inheritDoc
     */
    @Override
    public Record getRecord(String cloudId) throws RecordNotExistsException {
        Record aRecord = null;
        if (uis.existsCloudId(cloudId)) {
            aRecord = recordDAO.getRecord(cloudId);
            if (aRecord.getRepresentations().isEmpty()) {
                throw new RecordNotExistsException(cloudId);
            }
        } else {
            throw new RecordNotExistsException(cloudId);
        }
        return aRecord;
    }


    /**
     * @inheritDoc
     */
    @Override
    public void deleteRecord(String cloudId) throws RecordNotExistsException, RepresentationNotExistsException {

        if (uis.existsCloudId(cloudId)) {
            List<Representation> allRecordRepresentationsInAllVersions = recordDAO.listRepresentationVersions(cloudId);
            if (allRecordRepresentationsInAllVersions.isEmpty()) {
                throw new RepresentationNotExistsException(String.format(
                        "No representation found for given cloudId %s", cloudId));
            }

            sortByProviderId(allRecordRepresentationsInAllVersions);

            for (Representation repVersion : allRecordRepresentationsInAllVersions) {
                removeFilesFromRepresentationVersion(cloudId, repVersion);
                removeRepresentationAssignmentFromDataSets(cloudId, repVersion);
                deleteRepresentationRevision(cloudId, repVersion);
                recordDAO.deleteRepresentation(cloudId, repVersion.getRepresentationName(), repVersion.getVersion());
            }
        } else {
            throw new RecordNotExistsException(cloudId);
        }
    }

    private void removeFilesFromRepresentationVersion(String cloudId, Representation repVersion) {

        for (File f : repVersion.getFiles()) {
            try {
                contentDAO.deleteContent(FileUtils.generateKeyForFile(cloudId, repVersion.getRepresentationName(),
                        repVersion.getVersion(), f.getFileName()), f.getFileStorage());
            } catch (FileNotExistsException ex) {
                LOGGER.warn(
                        "File '{}' was found in representation {}-{}-{} but no content of such file was found",
                        f.getFileName(), cloudId, repVersion.getRepresentationName(), repVersion.getVersion());
            }
        }
    }

    /**
     * @inheritDoc
     */
    @Override
    public void deleteRepresentation(String globalId, String schema) throws RepresentationNotExistsException {

        List<Representation> listRepresentations = recordDAO.listRepresentationVersions(globalId, schema);
        if(listRepresentations == null) {
            throw new RepresentationNotExistsException();
        }

        sortByProviderId(listRepresentations);

        for (Representation rep : listRepresentations) {
            removeFilesFromRepresentationVersion(globalId, rep);
            removeRepresentationAssignmentFromDataSets(globalId, rep);
            deleteRepresentationRevision(globalId, rep);
        }
        recordDAO.deleteRepresentation(globalId, schema);
    }

    private void deleteRepresentationRevision(String globalId, Representation rep) {
        for (Revision r : rep.getRevisions()) {
            recordDAO.deleteRepresentationRevision(globalId, rep.getRepresentationName(), rep.getVersion(), r.getRevisionProviderId(), r.getRevisionName(), r.getCreationTimeStamp());
        }
    }

    @Override
    public Representation createRepresentation(String globalId, String schema, String providerId)
            throws RecordNotExistsException, ProviderNotExistsException {
        return createRepresentation(globalId, schema, providerId, null);
    }

    /**
     * @inheritDoc
     */
    @Override
    public Representation createRepresentation(String cloudId, String representationName, String providerId, UUID version)
            throws ProviderNotExistsException, RecordNotExistsException {
        Date now = Calendar.getInstance().getTime();
        if (version == null) {
            version = generateTimeUUID();
        }
        // check if data provider exists
        if (uis.getProvider(providerId) == null) {
            throw new ProviderNotExistsException(String.format("Provider %s does not exist.", providerId));
        }
        LOGGER.debug("Confirmed provider, id={} exists.", providerId);

        boolean cloudExists = uis.existsCloudId(cloudId);
        LOGGER.debug("Confirmed cloudId={} exists.", cloudId);

        if (cloudExists) {
            Representation representation =
                    recordDAO.createRepresentation(cloudId, representationName, providerId, now, version);
            LOGGER.debug("Created representation cloudid={}, representationName={}, providerId={}, version={}"
                    , cloudId, representationName, providerId, version);
            return representation;
        } else {
            throw new RecordNotExistsException(cloudId);
        }
    }


    /**
     * @inheritDoc
     */
    @Override
    public Representation getRepresentation(String globalId, String schema)
            throws RepresentationNotExistsException {
        Representation rep = recordDAO.getLatestPersistentRepresentation(globalId, schema);
        if (rep == null) {
            throw new RepresentationNotExistsException();
        } else {
            return rep;
        }
    }


    /**
     * @inheritDoc
     */
    @Override
    public Representation getRepresentation(String globalId, String schema, String version)
            throws RepresentationNotExistsException {
        Representation rep = recordDAO.getRepresentation(globalId, schema, version);
        if (rep == null) {
            throw new RepresentationNotExistsException();
        } else {
            LOGGER.debug("Loaded representation {}", rep);
            return rep;
        }
    }


    /**
     * @inheritDoc
     */
    @Override
    public void deleteRepresentation(String globalId, String schema, String version)
            throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException {
        Representation rep = recordDAO.getRepresentation(globalId, schema, version);
        if (rep == null) {
            throw new RepresentationNotExistsException();
        }

        removeFilesFromRepresentationVersion(globalId, rep);
        removeRepresentationAssignmentFromDataSets(globalId, rep);
        deleteRepresentationRevision(globalId, rep);
        recordDAO.deleteRepresentation(globalId, schema, version);

    }

    private void removeRepresentationAssignmentFromDataSets(String globalId, Representation representation) throws RepresentationNotExistsException {
        Collection<CompoundDataSetId> compoundDataSetIds = dataSetDAO.getDataSetAssignmentsByRepresentationVersion(globalId, representation.getRepresentationName(), representation.getVersion());
        if (!compoundDataSetIds.isEmpty()) {
            for (CompoundDataSetId compoundDataSetId : compoundDataSetIds) {
                try {
                    dataSetService.removeAssignment(compoundDataSetId.getDataSetProviderId(), compoundDataSetId.getDataSetId(), globalId, representation.getRepresentationName(), representation.getVersion());
                } catch (DataSetNotExistsException e) {
                    //Nothing to do, skip exception
                }
            }
        }
    }


    /**
     * @inheritDoc
     */
    @Override
    public Representation persistRepresentation(String globalId, String schema, String version)
            throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException,
            CannotPersistEmptyRepresentationException {
        Date now = Calendar.getInstance().getTime();
        Representation rep = recordDAO.getRepresentation(globalId, schema, version);
        if (rep == null) {
            throw new RepresentationNotExistsException();
        } else if (rep.isPersistent()) {
            throw new CannotModifyPersistentRepresentationException();
        }
        List<File> recordFiles = recordDAO.getFilesForRepresentation(globalId, schema, version);
        if (recordFiles == null) {
            throw new RepresentationNotExistsException();

        } else if (recordFiles.isEmpty()) {
            throw new CannotPersistEmptyRepresentationException();
        }

        recordDAO.persistRepresentation(globalId, schema, version, now);

        rep.setPersistent(true);
        rep.setCreationDate(now);

        return rep;
    }


    /**
     * @inheritDoc
     */
    @Override
    public List<Representation> listRepresentationVersions(String globalId, String schema)
            throws RepresentationNotExistsException {

        List<Representation> result = recordDAO.listRepresentationVersions(globalId, schema);
        if(result == null) {
            throw new RepresentationNotExistsException();
        }
        return result;
    }


    /**
     * @inheritDoc
     */
    @Override
    public boolean putContent(String globalId, String schema, String version, File file, InputStream content)
            throws CannotModifyPersistentRepresentationException, RepresentationNotExistsException {
        DateTime now = new DateTime();
        Representation representation = getRepresentation(globalId, schema, version);
        if (representation.isPersistent()) {
            throw new CannotModifyPersistentRepresentationException();
        }
        boolean isCreate = true; // if it is create file operation or update
        // content
        for (File f : representation.getFiles()) {
            if (f.getFileName().equals(file.getFileName())) {
                isCreate = false;
                break;
            }
        }
        String keyForFile = FileUtils.generateKeyForFile(globalId, schema, version, file.getFileName());
        PutResult result;
        try {
            result = contentDAO.putContent(keyForFile, content, file.getFileStorage());
            LOGGER.debug("Stored content for file: {} version: {}", file.getFileName(), version);
        } catch (IOException ex) {
            throw new SystemException(ex);
        }

        file.setMd5(result.getMd5());
        DateTimeFormatter fmt = ISODateTimeFormat.dateTime();
        file.setDate(fmt.print(now));
        file.setContentLength(result.getContentLength());
        recordDAO.addOrReplaceFileInRepresentation(globalId, schema, version, file);
        LOGGER.debug("Updated file information in representation: {}", representation);

        for (Revision revision : representation.getRevisions()) {
            // update information in extra table
            recordDAO.addOrReplaceFileInRepresentationRevision(globalId, schema, version, revision.getRevisionProviderId(), revision.getRevisionName(), revision.getCreationTimeStamp(), file);
            LOGGER.debug("Updated file information in revision: {}", revision);
        }

        return isCreate;
    }


    /**
     * @inheritDoc
     * @return
     */
    @Override
    public Consumer<OutputStream> getContent(String globalId, String schema, String version, String fileName, long rangeStart,
                                             long rangeEnd)
            throws FileNotExistsException, WrongContentRangeException, RepresentationNotExistsException {
        File file = getFile(globalId, schema, version, fileName);
        if (rangeStart > file.getContentLength() - 1) {
            throw new WrongContentRangeException("Start range must be less than file length");
        }

        if (rangeEnd > file.getContentLength() - 1) {
            throw new WrongContentRangeException("End range must be less than file length");
        }
        return os-> {
            try {
                contentDAO.getContent(FileUtils.generateKeyForFile(globalId, schema, version, fileName), rangeStart,
                        rangeEnd, os, file.getFileStorage());
            } catch (FileNotExistsException | IOException ex) {
                throw new SystemException(ex);
            }
        };

    }


    /**
     * @inheritDoc
     */
    @Override
    public String getContent(String globalId, String schema, String version, String fileName, OutputStream os)
            throws FileNotExistsException, RepresentationNotExistsException {
        Representation rep = getRepresentation(globalId, schema, version);
        File file = findFileInRepresentation(rep, fileName);
        try {
            contentDAO.getContent(FileUtils.generateKeyForFile(globalId, schema, version, fileName), -1, -1, os,
                    file.getFileStorage());
        } catch (IOException ex) {
            throw new SystemException(ex);
        }
        return file.getMd5();
    }


    /**
     * @inheritDoc
     */
    @Override
    public void deleteContent(String globalId, String schema, String version, String fileName)
            throws FileNotExistsException, CannotModifyPersistentRepresentationException,
            RepresentationNotExistsException {
        Representation representation = getRepresentation(globalId, schema, version);
        if (representation.isPersistent()) {
            throw new CannotModifyPersistentRepresentationException();
        }
        recordDAO.removeFileFromRepresentation(globalId, schema, version, fileName);
        recordDAO.removeFileFromRepresentationRevisionsTable(representation, fileName);
        File file = findFileInRepresentation(representation, fileName);
        contentDAO.deleteContent(FileUtils.generateKeyForFile(globalId, schema, version, fileName), file.getFileStorage());
    }


    /**
     * @inheritDoc
     */
    @Override
    public Representation copyRepresentation(String globalId, String schema, String version)
            throws RepresentationNotExistsException {
        Date now = Calendar.getInstance().getTime();
        Representation srcRep = recordDAO.getRepresentation(globalId, schema, version);
        if (srcRep == null) {
            throw new RepresentationNotExistsException();
        }

        Representation copiedRep = recordDAO.createRepresentation(globalId, schema, srcRep.getDataProvider(), now, generateTimeUUID());
        for (File srcFile : srcRep.getFiles()) {
            File copiedFile = new File(srcFile);
            try {
                contentDAO.copyContent(FileUtils.generateKeyForFile(globalId, schema, version, srcFile.getFileName()),
                        FileUtils.generateKeyForFile(globalId, schema, copiedRep.getVersion(), copiedFile.getFileName()),
                        srcFile.getFileStorage());
            } catch (FileNotExistsException ex) {
                LOGGER.warn("File '{}' was found in representation {}-{}-{} but no content of such file was found",
                        srcFile.getFileName(), globalId, schema, version);
            } catch (FileAlreadyExistsException ex) {
                LOGGER.warn("File '{}' already exists in newly created representation? {}-{}-{}",
                        copiedFile.getFileName(), globalId, schema, copiedRep.getVersion());
            } catch (IOException e) {
                LOGGER.error(Throwables.getStackTraceAsString(e));
            }
            recordDAO.addOrReplaceFileInRepresentation(globalId, schema, copiedRep.getVersion(), copiedFile);
        }
        // get version after all modifications
        return recordDAO.getRepresentation(globalId, schema, copiedRep.getVersion());
    }


    /**
     * @inheritDoc
     */
    @Override
    public File getFile(String globalId, String schema, String version, String fileName)
            throws RepresentationNotExistsException, FileNotExistsException {
        final Representation rep = getRepresentation(globalId, schema, version);
        return findFileInRepresentation(rep, fileName);
    }

    private File findFileInRepresentation(Representation representation, String fileName) throws FileNotExistsException {
        for (File file : representation.getFiles()) {
            if (file.getFileName().equals(fileName)) {
                return file;
            }
        }
        throw new FileNotExistsException();
    }


    private static void sortByProviderId(List<Representation> input) {
        Collections.sort(input, new Comparator<Representation>() {

            @Override
            public int compare(Representation r1, Representation r2) {
                return r1.getDataProvider().compareToIgnoreCase(r2.getDataProvider());
            }
        });
    }


    /**
     * @inheritDoc
     */
    @Override
    public void addRevision(String globalId, String schema, String version, Revision revision) throws RevisionIsNotValidException {
        recordDAO.addOrReplaceRevisionInRepresentation(globalId, schema, version, revision);
    }

    @Override
    public List<RepresentationRevisionResponse> getRepresentationRevisions(String globalId, String schema, String revisionProviderId, String revisionName, Date revisionTimestamp) {
        return recordDAO.getRepresentationRevisions(globalId, schema, revisionProviderId, revisionName, revisionTimestamp);
    }

    @Override
    public void insertRepresentationRevision(String globalId, String schema, String revisionProviderId, String revisionName, String versionId, Date revisionTimestamp) {
        // add additional association between representation version and revision
        Representation representation = recordDAO.getRepresentation(globalId, schema, versionId);
        recordDAO.addRepresentationRevision(globalId, schema, versionId, revisionProviderId, revisionName, revisionTimestamp);
        for (File file : representation.getFiles())
            recordDAO.addOrReplaceFileInRepresentationRevision(globalId, schema, versionId, revisionProviderId, revisionName, revisionTimestamp, file);
    }

    /**
     * @inheritDoc
     */
    @Override
    public Revision getRevision(String globalId, String schema, String version, String revisionKey)
            throws RevisionNotExistsException, RepresentationNotExistsException {
        Representation rep = getRepresentation(globalId, schema, version);
        for (Revision revision : rep.getRevisions()) {
            if (revision != null && RevisionUtils.getRevisionKey(revision).equals(revisionKey)) {
                return revision;
            }
        }
        throw new RevisionNotExistsException();
    }

    private static UUID generateTimeUUID() {
        return UUID.fromString(new com.eaio.uuid.UUID().toString());
    }

}
