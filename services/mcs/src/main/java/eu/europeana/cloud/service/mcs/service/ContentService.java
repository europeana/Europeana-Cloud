package eu.europeana.cloud.service.mcs.service;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;

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
