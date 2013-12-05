package eu.europeana.cloud.service.mcs.exception;

/**
 * Thrown when there is attempt to get non existing file from a specific representation version.
 */
public class FileNotExistsException extends RuntimeException {

    public FileNotExistsException() {
    }


    public FileNotExistsException(String message) {
        super(message);
    }
}
