package eu.europeana.cloud.service.mcs.inmemory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.VersionNotExistsException;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;

/**
 * InMemoryContentServiceImpl
 */
@Service
public class InMemoryRecordService implements RecordService {

    @Autowired
    private InMemoryRecordDAO recordDAO;

    @Autowired
    private InMemoryContentDAO contentDAO;


    @Override
    public Record getRecord(String globalId)
            throws RecordNotExistsException {
        return recordDAO.getRecord(globalId);
    }


    @Override
    public void deleteRecord(String globalId)
            throws RecordNotExistsException {
        recordDAO.deleteRecord(globalId);
    }


    @Override
    public void deleteRepresentation(String globalId, String representationName)
            throws RecordNotExistsException, RepresentationNotExistsException {
        recordDAO.deleteRepresentation(globalId, representationName);
    }


    @Override
    public Representation createRepresentation(String globalId, String representationName, String providerId) {
        return recordDAO.createRepresentation(globalId, representationName, providerId);
    }


    @Override
    public Representation getRepresentation(String globalId, String representationName)
            throws RecordNotExistsException, RepresentationNotExistsException {
        return recordDAO.getRepresentation(globalId, representationName, null);
    }


    @Override
    public Representation getRepresentation(String globalId, String representationName, String version)
            throws RecordNotExistsException, RepresentationNotExistsException, VersionNotExistsException {
        return recordDAO.getRepresentation(globalId, representationName, version);
    }


    @Override
    public void deleteRepresentation(String globalId, String representationName, String version)
            throws RecordNotExistsException, RepresentationNotExistsException, VersionNotExistsException {
        recordDAO.deleteRepresentation(globalId, representationName, version);
    }


    @Override
    public Representation persistRepresentation(String globalId, String representationName, String version)
            throws RecordNotExistsException, RepresentationNotExistsException, VersionNotExistsException, CannotModifyPersistentRepresentationException {
        return recordDAO.persistRepresentation(globalId, representationName, version);
    }


    @Override
    public List<Representation> listRepresentationVersions(String globalId, String representationName)
            throws RecordNotExistsException, RepresentationNotExistsException {
        return recordDAO.listRepresentationVersions(globalId, representationName);
    }


    @Override
    public boolean putContent(String globalId, String representationName, String version, File file, InputStream content)
            throws FileAlreadyExistsException, IOException {
        Representation representation = recordDAO.getRepresentation(globalId, representationName, version);
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
        
        contentDAO.putContent(globalId, representationName, version, file, content);
        recordDAO.addOrReplaceFileInRepresentation(globalId, representationName, version, file);
        return isCreate;
    }


    @Override
    public void getContent(String globalId, String representationName, String version, String fileName, long rangeStart, long rangeEnd, OutputStream os)
            throws FileNotExistsException, IOException {
        contentDAO.getContent(globalId, representationName, version, fileName, rangeStart, rangeEnd, os);
    }


    @Override
    public String getContent(String globalId, String representationName, String version, String fileName, OutputStream os)
            throws FileNotExistsException, IOException {
        Representation rep = getRepresentation(globalId, representationName, version);
        String md5 = null;
        for (File f : rep.getFiles()) {
            if (fileName.equals(f.getFileName())) {
                md5 = f.getMd5();
            }
        }
        if (md5 == null) {
            throw new FileNotExistsException();
        }
        contentDAO.getContent(globalId, representationName, version, fileName, -1, -1, os);
        return md5;
    }


    @Override
    public void deleteContent(String globalId, String representationName, String version, String fileName)
            throws FileNotExistsException {
        recordDAO.removeFileFromRepresentation(globalId, representationName, version, fileName);
        contentDAO.deleteContent(globalId, representationName, version, fileName);
    }
}
