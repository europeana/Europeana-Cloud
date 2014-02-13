package eu.europeana.cloud.service.mcs.exception;

/**
 * Thrown if there was attempt to use representation that does not exist (or representation in version that does not
 * exist).
 */
public class RepresentationAlreadyInSet extends MCSException {

    /**
     * Constructs a RepresentationNotExistsException with no specified detail message.
     */
    public RepresentationAlreadyInSet() {
    }


    /**
     * Constructs a RepresentationNotExistsException with the specified detail message.
     * 
     * @param message
     *            the detail message
     */
    public RepresentationAlreadyInSet(String message) {
        super(message);
    }


    /**
     * Constructs a RepresentationNotExistsException with the specified Throwable.
     * 
     * @param cause
     *            the cause
     */
    public RepresentationAlreadyInSet(Throwable cause) {
        super(cause);
    }

}
