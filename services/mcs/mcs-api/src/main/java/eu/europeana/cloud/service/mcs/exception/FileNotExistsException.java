package eu.europeana.cloud.service.mcs.exception;

/**
 * RecordNotExistsException
 */
public class FileNotExistsException extends RuntimeException {

    public FileNotExistsException() {
    }


    public FileNotExistsException(String message) {
        super(message);
    }
}
