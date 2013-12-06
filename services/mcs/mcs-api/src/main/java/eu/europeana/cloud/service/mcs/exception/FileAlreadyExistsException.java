package eu.europeana.cloud.service.mcs.exception;

/**
 * Thrown whene there is attempt to create a file which is already attached to a specific representation version.
 */
public class FileAlreadyExistsException extends RuntimeException {

    public FileAlreadyExistsException() {
    }


    public FileAlreadyExistsException(String message) {
        super(message);
    }
}
