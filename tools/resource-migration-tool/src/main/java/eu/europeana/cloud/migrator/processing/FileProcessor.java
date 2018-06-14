package eu.europeana.cloud.migrator.processing;

import java.io.File;
import java.net.URI;

public interface FileProcessor {

    /**
     * Process file in the given location. Return input stream of the processed file.
     *
     * @param fileURI URI to the file that should be processed
     * @return a file object - the result of the processing
     */
    File process(URI fileURI);
}
