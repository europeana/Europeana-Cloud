package eu.europeana.cloud.service.mcs.exception;

/**
 * RecordNotExistsException
 */
public class FileAlreadyExistsException extends RuntimeException {

    public FileAlreadyExistsException() {
    }


    public FileAlreadyExistsException(String message) {
        super(message);
    }
}
