package eu.europeana.cloud.service.mcs.exception;

/**
 * Thrown when there was attempt to modify persistent representation version.
 */
public class CannotModifyPersistentRepresentationException extends BaseMCSException {
    /**
     * Constructs a CannotModifyPersistentRepresentationException with no specified detail message.
     */
    public CannotModifyPersistentRepresentationException() {
        super();
    }

    /**
     * Constructs a CannotModifyPersistentRepresentationException with the specified detail message.
     * 
     * @param message
     *            the detail message
     */
    public CannotModifyPersistentRepresentationException(String message) {
        super(message);
    }

    /**
     * Constructs a CannotModifyPersistentRepresentationException with the specified detail message.
     * 
     * @param message
     *            the detail message
     * @param e
     *            exception
     */
    public CannotModifyPersistentRepresentationException(String message, Exception e) {
        super(message, e);
    }
}
