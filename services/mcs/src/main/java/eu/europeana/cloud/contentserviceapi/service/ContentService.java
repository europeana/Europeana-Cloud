package eu.europeana.cloud.contentserviceapi.service;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import eu.europeana.cloud.contentserviceapi.exception.FileAlreadyExistsException;
import eu.europeana.cloud.contentserviceapi.exception.FileNotExistsException;
import eu.europeana.cloud.contentserviceapi.model.File;
import eu.europeana.cloud.contentserviceapi.model.Representation;

/**
 * ContentService
 */
public interface ContentService {

    void insertContent(Representation rep, File file, InputStream content)
            throws FileAlreadyExistsException, IOException;


    void writeContent(Representation rep, File file, long rangeStart, long rangeEnd, OutputStream os)
            throws FileNotExistsException, IOException;


    void writeContent(Representation rep, File file, OutputStream os)
            throws FileNotExistsException, IOException;
}
