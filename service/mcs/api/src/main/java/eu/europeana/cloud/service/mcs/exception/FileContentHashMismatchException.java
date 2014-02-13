package eu.europeana.cloud.service.mcs.exception;

/**
 * Thrown when declared file's hash is different than actual hash.
 */
public class FileContentHashMismatchException extends MCSException {

    /**
     * Constructs a FileContentHashMismatchException with no specified detail message.
     */
    public FileContentHashMismatchException() {
    }


    /**
     * Constructs a FileContentHashMismatchException with the specified detail message.
     * 
     * @param message
     *            the detail message
     */
    public FileContentHashMismatchException(String message) {
        super(message);
    }

}
