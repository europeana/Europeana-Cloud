package eu.europeana.cloud.service.mcs.exception;

/**
 * Thrown when there was attempt to persist representation with no files attached.
 */
public class CannotPersistEmptyRepresentationException extends Exception {

    public CannotPersistEmptyRepresentationException(String message) {
        super(message);
    }


    public CannotPersistEmptyRepresentationException() {
    }
}
