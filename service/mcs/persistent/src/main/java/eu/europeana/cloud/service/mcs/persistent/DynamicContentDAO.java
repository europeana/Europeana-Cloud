package eu.europeana.cloud.service.mcs.persistent;

import eu.europeana.cloud.service.mcs.Storage;
import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.persistent.exception.ContentDaoNotFoundException;
import eu.europeana.cloud.service.mcs.persistent.swift.ContentDAO;
import eu.europeana.cloud.service.mcs.persistent.swift.PutResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.EnumMap;
import java.util.Map;

import static org.apache.commons.lang.Validate.notEmpty;


/**
 * Proxy that switch contentDAO based on {@link Storage} value.
 *
 * @author krystian.
 */
@Repository
public class DynamicContentDAO {

    private final Map<Storage,ContentDAO> contentDAOs = new EnumMap<>(Storage.class);

    @Autowired
    public DynamicContentDAO(Map<Storage,ContentDAO> contentDAOs) {
        notEmpty(contentDAOs);
        this.contentDAOs.putAll(contentDAOs);
    }

    public void copyContent(String sourceObjectId, String trgObjectId, Storage stored) throws
            FileNotExistsException, FileAlreadyExistsException, IOException {
        getContentDAO(stored).copyContent(sourceObjectId,trgObjectId);
    }


    public void deleteContent(String fileName, Storage stored) throws FileNotExistsException {
        getContentDAO(stored).deleteContent(fileName);
    }

    public void getContent(String fileName, long start, long end, OutputStream os, Storage stored) throws
            IOException, FileNotExistsException {
        getContentDAO(stored).getContent(fileName,start,end,os);
    }

    public PutResult putContent(String fileName, InputStream data, Storage stored) throws IOException {
        return getContentDAO(stored).putContent(fileName,data);
    }

    private ContentDAO getContentDAO(final Storage storage) {
        ContentDAO dao = contentDAOs.get(storage);
        if (dao == null) {
            throw new ContentDaoNotFoundException(
                    "Specified storage \"" + storage + "\" has not been defined in " + this + "!");
        }
        return dao;
    }
}
