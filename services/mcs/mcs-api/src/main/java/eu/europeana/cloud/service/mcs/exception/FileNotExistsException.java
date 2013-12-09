package eu.europeana.cloud.service.mcs.exception;

/**
 * Thrown when there is attempt to get non existing file from a specific representation version.
 */
public class FileNotExistsException extends Exception {

    public FileNotExistsException() {
    }


    public FileNotExistsException(String message) {
        super(message);
    }
}
