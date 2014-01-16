package eu.europeana.cloud.service.mcs.exception;

/**
 * Thrown if there was attempt to use representation that does not exist (or representation in version that does not
 * exist).
 */
public class RepresentationNotExistsException extends Exception {

    /**
     * Constructs a RepresentationNotExistsException with no specified detail message.
     */
    public RepresentationNotExistsException() {
    }


    /**
     * Constructs a RepresentationNotExistsException with the specified detail message.
     * 
     * @param message
     *            the detail message
     */
    public RepresentationNotExistsException(String message) {
        super(message);
    }


    /**
     * Constructs a RepresentationNotExistsException with the specified Throwable.
     * 
     * @param cause
     *            the cause
     */
    public RepresentationNotExistsException(Throwable cause) {
        super(cause);
    }

}
