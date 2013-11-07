package eu.europeana.cloud.service.mcs.exception;

/**
 * FileHashMismatchException
 */
public class FileContentHashMismatchException extends RuntimeException {

    public FileContentHashMismatchException() {
    }


    public FileContentHashMismatchException(String message) {
        super(message);
    }


}
