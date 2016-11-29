package eu.europeana.cloud.service.mcs.persistent;

import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraContentDAO;
import eu.europeana.cloud.service.mcs.persistent.swift.ContentDAO;
import eu.europeana.cloud.service.mcs.persistent.swift.PutResult;
import eu.europeana.cloud.service.mcs.persistent.swift.SwiftContentDAO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Proxy that switch contentDAO based on boolean value.
 *
 * @author krystian.
 */
@Repository
public class DynamicContentDAO {

    @Autowired
    private SwiftContentDAO swiftContentDAO;

    @Autowired
    private CassandraContentDAO cassandraContentDAO;


    public void copyContent(String sourceObjectId, String trgObjectId, boolean storeInDb) throws
            FileNotExistsException, FileAlreadyExistsException, IOException {
        getContentDAO(storeInDb).copyContent(sourceObjectId,trgObjectId);
    }


    public void deleteContent(String fileName, boolean storedInDb) throws FileNotExistsException {
        getContentDAO(storedInDb).deleteContent(fileName);
    }

    public void getContent(String fileName, long start, long end, OutputStream os, boolean storedInDb) throws IOException, FileNotExistsException {
        getContentDAO(storedInDb).getContent(fileName,start,end,os);

    }

    public PutResult putContent(String fileName, InputStream data, boolean storedInDb) throws IOException {
        return getContentDAO(storedInDb).putContent(fileName,data);
    }

    private ContentDAO getContentDAO(final boolean storeInDb) {
        return storeInDb == true ? cassandraContentDAO : swiftContentDAO;
    }
}
