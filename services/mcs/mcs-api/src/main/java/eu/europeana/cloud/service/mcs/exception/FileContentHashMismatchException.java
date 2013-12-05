package eu.europeana.cloud.service.mcs.exception;

/**
 * Thrown when declared file's hash is different than actual hash.
 */
public class FileContentHashMismatchException extends RuntimeException {

    public FileContentHashMismatchException() {
    }


    public FileContentHashMismatchException(String message) {
        super(message);
    }

}
