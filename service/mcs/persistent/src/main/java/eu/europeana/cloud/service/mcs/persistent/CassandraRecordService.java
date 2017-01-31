package eu.europeana.cloud.service.mcs.persistent;

import eu.europeana.cloud.common.model.*;
import eu.europeana.cloud.common.utils.FileUtils;
import eu.europeana.cloud.common.utils.RevisionUtils;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.exception.*;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraDataSetDAO;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraRecordDAO;
import eu.europeana.cloud.service.mcs.persistent.exception.SystemException;
import eu.europeana.cloud.service.mcs.persistent.swift.PutResult;
import eu.europeana.cloud.service.mcs.persistent.swift.SwiftContentDAO;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Implementation of record service using Cassandra as storage.
 */
@Service
public class CassandraRecordService implements RecordService {

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraRecordService.class);

    @Autowired
    private CassandraRecordDAO recordDAO;


    @Autowired
    private CassandraDataSetService dataSetService;

    @Autowired
    private CassandraDataSetDAO dataSetDAO;

    @Autowired
    private SwiftContentDAO contentDAO;

    @Autowired
    private SolrRepresentationIndexer representationIndexer;

    @Autowired
    private UISClientHandler uis;


    /**
     * @inheritDoc
     */
    @Override
    public Record getRecord(String cloudId)
            throws RecordNotExistsException {
        Record record = null;
        if (uis.existsCloudId(cloudId)) {
            record = recordDAO.getRecord(cloudId);
            if (record.getRepresentations().isEmpty()) {
                throw new RecordNotExistsException(cloudId);
            }
        } else {
            throw new RecordNotExistsException(cloudId);
        }
        return record;
    }


    /**
     * @inheritDoc
     */
    @Override
    public void deleteRecord(String cloudId)
            throws RecordNotExistsException, RepresentationNotExistsException {
        if (uis.existsCloudId(cloudId)) {
            List<Representation> allRecordRepresentationsInAllVersions = recordDAO.listRepresentationVersions(cloudId);
            if (allRecordRepresentationsInAllVersions.isEmpty()) {
                throw new RepresentationNotExistsException(String.format(
                        "No representation found for given cloudId %s", cloudId));
            }

            sortByProviderId(allRecordRepresentationsInAllVersions);

            String dPId = null;

            for (Representation repVersion : allRecordRepresentationsInAllVersions) {
                if (!(repVersion.getDataProvider()).equalsIgnoreCase(dPId)) {
                    dPId = repVersion.getDataProvider();
                    representationIndexer.removeRecordRepresentations(cloudId, uis.getProvider(dPId).getPartitionKey());
                }
                for (File f : repVersion.getFiles()) {
                    try {
                        contentDAO.deleteContent(FileUtils.generateKeyForFile(cloudId, repVersion.getRepresentationName(),
                                repVersion.getVersion(), f.getFileName()));
                    } catch (FileNotExistsException ex) {
                        LOGGER.warn(
                                "File {} was found in representation {}-{}-{} but no content of such file was found",
                                f.getFileName(), cloudId, repVersion.getRepresentationName(), repVersion.getVersion());
                    }
                }
            }
            recordDAO.deleteRecord(cloudId);
        } else {
            throw new RecordNotExistsException(cloudId);
        }
    }


    /**
     * @inheritDoc
     */
    @Override
    public void deleteRepresentation(String globalId, String schema)
            throws RepresentationNotExistsException {

        List<Representation> listRepresentations = recordDAO.listRepresentationVersions(globalId, schema);

        sortByProviderId(listRepresentations);

        String dPId = null;

        for (Representation rep : listRepresentations) {
            if (!(rep.getDataProvider()).equalsIgnoreCase(dPId)) {
                // send only one message per DataProvider
                dPId = rep.getDataProvider();
                representationIndexer.removeRepresentation(globalId, schema, uis.getProvider(dPId).getPartitionKey());
            }
            for (File f : rep.getFiles()) {
                try {
                    contentDAO.deleteContent(FileUtils.generateKeyForFile(globalId, schema, rep.getVersion(), f.getFileName()));
                } catch (FileNotExistsException ex) {
                    LOGGER.warn("File {} was found in representation {}-{}-{} but no content of such file was found",
                            f.getFileName(), globalId, rep.getRepresentationName(), rep.getVersion());
                }
            }

            Collection<CompoundDataSetId> compoundDataSetIds = dataSetDAO.getDataSetAssignmentsByRepresentationVersion(globalId, schema, rep.getVersion());
            if (!compoundDataSetIds.isEmpty()) {
                for (CompoundDataSetId compoundDataSetId : compoundDataSetIds) {
                    try {
                        dataSetService.removeAssignment(compoundDataSetId.getDataSetProviderId(), compoundDataSetId.getDataSetId(), globalId, schema, rep.getVersion());
                    } catch (DataSetNotExistsException e) {
                    }
                }
            }
        }
        recordDAO.deleteRepresentation(globalId, schema);
    }


    /**
     * @inheritDoc
     */
    @Override
    public Representation createRepresentation(String cloudId, String representationName, String providerId)
            throws ProviderNotExistsException, RecordNotExistsException {

        Date now = new Date();
        DataProvider dataProvider;
        // check if data provider exists
        if ((dataProvider = uis.getProvider(providerId)) == null) {
            throw new ProviderNotExistsException(String.format("Provider %s does not exist.", providerId));
        }
        if (uis.existsCloudId(cloudId)) {
            Representation rep = recordDAO.createRepresentation(cloudId, representationName, providerId, now);
            representationIndexer.insertRepresentation(rep, dataProvider.getPartitionKey());
            return rep;
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
        if (rep.isPersistent()) {
            throw new CannotModifyPersistentRepresentationException();
        }

        representationIndexer.removeRepresentationVersion(version, uis.getProvider(rep.getDataProvider())
                .getPartitionKey());

        for (File f : rep.getFiles()) {
            try {
                contentDAO.deleteContent(FileUtils.generateKeyForFile(globalId, schema, version, f.getFileName()));
            } catch (FileNotExistsException ex) {
                LOGGER.warn("File {} was found in representation {}-{}-{} but no content of such file was found",
                        f.getFileName(), globalId, rep.getRepresentationName(), rep.getVersion());
            }
        }

        Collection<CompoundDataSetId> compoundDataSetIds = dataSetDAO.getDataSetAssignmentsByRepresentationVersion(globalId, schema, version);
        if (!compoundDataSetIds.isEmpty()) {
            for (CompoundDataSetId compoundDataSetId : compoundDataSetIds) {
                try {
                    dataSetService.removeAssignment(compoundDataSetId.getDataSetProviderId(), compoundDataSetId.getDataSetId(), globalId, schema, version);
                } catch (DataSetNotExistsException e) {
                }
            }
        }
        recordDAO.deleteRepresentation(globalId, schema, version);

    }


    /**
     * @inheritDoc
     */
    @Override
    public Representation persistRepresentation(String globalId, String schema, String version)
            throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException,
            CannotPersistEmptyRepresentationException {
        Date now = new Date();
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

        representationIndexer.insertRepresentation(rep, uis.getProvider(rep.getDataProvider()).getPartitionKey());

        return rep;
    }


    /**
     * @inheritDoc
     */
    @Override
    public List<Representation> listRepresentationVersions(String globalId, String schema)
            throws RepresentationNotExistsException {
        return recordDAO.listRepresentationVersions(globalId, schema);
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
            result = contentDAO.putContent(keyForFile, content);
        } catch (IOException ex) {
            throw new SystemException(ex);
        }
        file.setMd5(result.getMd5());
        DateTimeFormatter fmt = ISODateTimeFormat.dateTime();
        file.setDate(fmt.print(now));
        file.setContentLength(result.getContentLength());
        recordDAO.addOrReplaceFileInRepresentation(globalId, schema, version, file);
        return isCreate;
    }


    /**
     * @inheritDoc
     */
    @Override
    public void getContent(String globalId, String schema, String version, String fileName, long rangeStart,
                           long rangeEnd, OutputStream os)
            throws FileNotExistsException, WrongContentRangeException, RepresentationNotExistsException {
        File file = getFile(globalId, schema, version, fileName);
        if (rangeStart > file.getContentLength() - 1) {
            throw new WrongContentRangeException("Start range must be less than file length");
        }
        try {
            contentDAO.getContent(FileUtils.generateKeyForFile(globalId, schema, version, fileName), rangeStart, rangeEnd, os);
        } catch (IOException ex) {
            throw new SystemException(ex);
        }
    }


    /**
     * @inheritDoc
     */
    @Override
    public String getContent(String globalId, String schema, String version, String fileName, OutputStream os)
            throws FileNotExistsException, RepresentationNotExistsException {
        Representation rep = getRepresentation(globalId, schema, version);
        String md5 = null;
        for (File f : rep.getFiles()) {
            if (fileName.equals(f.getFileName())) {
                md5 = f.getMd5();
            }
        }
        if (md5 == null) {
            throw new FileNotExistsException();
        }
        try {
            contentDAO.getContent(FileUtils.generateKeyForFile(globalId, schema, version, fileName), -1, -1, os);
        } catch (IOException ex) {
            throw new SystemException(ex);
        }
        return md5;
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
        contentDAO.deleteContent(FileUtils.generateKeyForFile(globalId, schema, version, fileName));
    }


    /**
     * @inheritDoc
     */
    @Override
    public Representation copyRepresentation(String globalId, String schema, String version)
            throws RepresentationNotExistsException {
        Date now = new Date();
        Representation srcRep = recordDAO.getRepresentation(globalId, schema, version);
        if (srcRep == null) {
            throw new RepresentationNotExistsException();
        }

        Representation copiedRep = recordDAO.createRepresentation(globalId, schema, srcRep.getDataProvider(), now);
        representationIndexer.insertRepresentation(copiedRep, uis.getProvider(srcRep.getDataProvider())
                .getPartitionKey());
        for (File srcFile : srcRep.getFiles()) {
            File copiedFile = new File(srcFile);
            try {
                contentDAO.copyContent(FileUtils.generateKeyForFile(globalId, schema, version, srcFile.getFileName()),
                        FileUtils.generateKeyForFile(globalId, schema, copiedRep.getVersion(), copiedFile.getFileName()));
            } catch (FileNotExistsException ex) {
                LOGGER.warn("File {} was found in representation {}-{}-{} but no content of such file was found",
                        srcFile.getFileName(), globalId, schema, version);
            } catch (FileAlreadyExistsException ex) {
                LOGGER.warn("File already exists in newly created representation?", copiedFile.getFileName(), globalId,
                        schema, copiedRep.getVersion());
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
        for (File f : rep.getFiles()) {
            if (f.getFileName().equals(fileName)) {
                return f;
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

    /**
     * @inheritDoc
     */
    @Override
    public Revision getRevision(String globalId, String schema, String version, String revisionKey)
            throws RevisionNotExistsException, RepresentationNotExistsException {
        Representation rep = getRepresentation(globalId, schema, version);
        for (Revision revision : rep.getRevisions()) {
            if (revision != null) {
                if (RevisionUtils.getRevisionKey(revision).equals(revisionKey)) {
                    return revision;
                }
            }
        }
        throw new RevisionNotExistsException();
    }


}
