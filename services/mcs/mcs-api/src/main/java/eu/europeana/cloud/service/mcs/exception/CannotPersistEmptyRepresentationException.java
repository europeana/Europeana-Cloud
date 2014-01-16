package eu.europeana.cloud.service.mcs.exception;

/**
 * Thrown when there was attempt to persist representation with no files attached.
 */
public class CannotPersistEmptyRepresentationException extends Exception {

    /**
     * Constructs a CannotPersistEmptyRepresentationException with the specified detail message.
     * 
     * @param message
     *            the detail message
     */
    public CannotPersistEmptyRepresentationException(String message) {
        super(message);
    }


    /**
     * Constructs a CannotPersistEmptyRepresentationException with no specified detail message.
     */
    public CannotPersistEmptyRepresentationException() {
    }
}
