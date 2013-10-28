package eu.europeana.cloud.service.mcs;

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

    /**
     * Puts content from input stream to storage service.
     * 
     * @param rep representation of record.
     * @param file file from the representation.
     * @param content input stream with content to be put into storage.
     * @throws FileAlreadyExistsException
     * @throws IOException 
     */
    void putContent(Representation rep, File file, InputStream content)
            throws FileAlreadyExistsException, IOException;


    void getContent(Representation rep, File file, long rangeStart, long rangeEnd, OutputStream os)
            throws FileNotExistsException, IOException;


    void getContent(Representation rep, File file, OutputStream os)
            throws FileNotExistsException, IOException;


    void deleteContent(String globalId, String representationName, String version, String fileName)
            throws FileNotExistsException;
}
