package eu.europeana.cloud.service.mcs.inmemory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.VersionNotExistsException;

/**
 * InMemoryContentServiceImpl
 */
@Service
public class InMemoryRecordService implements RecordService {

    @Autowired
    private InMemoryRecordDAO recordDAO;

    @Autowired
    private InMemoryContentDAO contentDAO;

    @Autowired
    private InMemoryDataSetDAO dataSetDAO;

    @Autowired
    private InMemoryDataProviderDAO dataProviderDAO;


    @Override
    public Record getRecord(String globalId)
            throws RecordNotExistsException {
        return recordDAO.getRecord(globalId);
    }


    @Override
    public void deleteRecord(String globalId)
            throws RecordNotExistsException {
        Record r = recordDAO.getRecord(globalId);
        for (Representation rep : r.getRepresentations()) {
            for (Representation repVersion : recordDAO.listRepresentationVersions(globalId, rep.getSchema())) {
                for (File f : repVersion.getFiles()) {
                    contentDAO.deleteContent(globalId, repVersion.getSchema(), repVersion.getVersion(), f.getFileName());
                }
            }
        }
        recordDAO.deleteRecord(globalId);
    }


    @Override
    public void deleteRepresentation(String globalId, String schema)
            throws RecordNotExistsException, RepresentationNotExistsException {
        for (Representation rep : recordDAO.listRepresentationVersions(globalId, schema)) {
            for (File f : rep.getFiles()) {
                contentDAO.deleteContent(globalId, schema, rep.getVersion(), f.getFileName());
            }
        }
        recordDAO.deleteRepresentation(globalId, schema);
    }


    @Override
    public Representation createRepresentation(String globalId, String representationName, String providerId)
            throws RecordNotExistsException, RepresentationNotExistsException, ProviderNotExistsException {
        dataProviderDAO.getProvider(providerId);
        return recordDAO.createRepresentation(globalId, representationName, providerId);
    }


    @Override
    public Representation getRepresentation(String globalId, String schema)
            throws RecordNotExistsException, RepresentationNotExistsException {
        return recordDAO.getRepresentation(globalId, schema, null);
    }


    @Override
    public Representation getRepresentation(String globalId, String schema, String version)
            throws RecordNotExistsException, RepresentationNotExistsException, VersionNotExistsException {
        return recordDAO.getRepresentation(globalId, schema, version);
    }


    @Override
    public void deleteRepresentation(String globalId, String schema, String version)
            throws RecordNotExistsException, RepresentationNotExistsException, VersionNotExistsException {
        Representation rep = recordDAO.getRepresentation(globalId, schema, version);
        for (File f : rep.getFiles()) {
            contentDAO.deleteContent(globalId, schema, version, f.getFileName());
        }
        recordDAO.deleteRepresentation(globalId, schema, version);
    }


    @Override
    public Representation persistRepresentation(String globalId, String schema, String version)
            throws RecordNotExistsException, RepresentationNotExistsException, VersionNotExistsException, CannotModifyPersistentRepresentationException {
        return recordDAO.persistRepresentation(globalId, schema, version);
    }


    @Override
    public List<Representation> listRepresentationVersions(String globalId, String schema)
            throws RecordNotExistsException, RepresentationNotExistsException {
        return recordDAO.listRepresentationVersions(globalId, schema);
    }


    @Override
    public boolean putContent(String globalId, String schema, String version, File file, InputStream content)
            throws FileAlreadyExistsException, IOException {
        Representation representation = recordDAO.getRepresentation(globalId, schema, version);
        if (representation.isPersistent()) {
            throw new CannotModifyPersistentRepresentationException();
        }

        boolean isCreate = true; // if it is create file operation or update content
        if (file.getFileName() == null) {
            file.setFileName(UUID.randomUUID().toString());
        } else {
            for (File f : representation.getFiles()) {
                if (f.getFileName().equals(file.getFileName())) {
                    isCreate = false;
                    break;
                }
            }
        }

        contentDAO.putContent(globalId, schema, version, file, content);
        recordDAO.addOrReplaceFileInRepresentation(globalId, schema, version, file);
        return isCreate;
    }


    @Override
    public void getContent(String globalId, String schema, String version, String fileName, long rangeStart, long rangeEnd, OutputStream os)
            throws FileNotExistsException, IOException {
        contentDAO.getContent(globalId, schema, version, fileName, rangeStart, rangeEnd, os);
    }


    @Override
    public String getContent(String globalId, String schema, String version, String fileName, OutputStream os)
            throws FileNotExistsException, IOException {
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
        contentDAO.getContent(globalId, schema, version, fileName, -1, -1, os);
        return md5;
    }


    @Override
    public void deleteContent(String globalId, String schema, String version, String fileName)
            throws FileNotExistsException {
        recordDAO.removeFileFromRepresentation(globalId, schema, version, fileName);
        contentDAO.deleteContent(globalId, schema, version, fileName);
    }


    @Override
    public Representation copyRepresentation(String globalId, String schema, String version)
            throws RecordNotExistsException, RepresentationNotExistsException, VersionNotExistsException, CannotModifyPersistentRepresentationException {
        Representation srcRep = recordDAO.getRepresentation(globalId, schema, version);
        Representation copiedRep = recordDAO.createRepresentation(globalId, schema, srcRep.getDataProvider());
        for (File srcFile : srcRep.getFiles()) {
            File copiedFile = new File(srcFile);
            copyRepresentation(globalId, schema, version);
            contentDAO.copyContent(globalId, schema, version, srcFile.getFileName(),
                    globalId, schema, copiedRep.getVersion(), copiedFile.getFileName());
            recordDAO.addOrReplaceFileInRepresentation(globalId, schema, copiedRep.getVersion(), copiedFile);
        }
        //get version after all modifications
        return recordDAO.getRepresentation(globalId, schema, copiedRep.getVersion());
    }


    @Override
    public ResultSlice<Representation> search(String providerId, String schema, String dataSetId, String thresholdParam, int limit) {
		if (thresholdParam != null) {
			throw new UnsupportedOperationException("Paging with threshold is not supported");
		}
		List<Representation> result;
        if (providerId != null && dataSetId != null) {
            // get all for dataset then filter for schema
            List<Representation> representationStubs = dataSetDAO.listDataSet(providerId, dataSetId);
            List<Representation> toReturn = new ArrayList<>(representationStubs.size());
            for (Representation stub : representationStubs) {
                if (schema == null || schema.equals(stub.getSchema())) {
                    Representation realContent = recordDAO.getRepresentation(stub.getRecordId(), stub.getSchema(), stub.getVersion());
                    toReturn.add(realContent);
                }
            }
            result= toReturn;
        } else {
            result= recordDAO.findRepresentations(providerId, schema);
        }
		result = result.subList(0, Math.min(limit, result.size()));
		return new ResultSlice<>(null, result);
    }
}
