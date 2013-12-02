package eu.europeana.cloud.service.mcs.exception;

/**
 * Thrown whren there was attempt to modify persistent representation version.
 */
public class CannotModifyPersistentRepresentationException extends RuntimeException {

    public CannotModifyPersistentRepresentationException(String message) {
        super(message);
    }


    public CannotModifyPersistentRepresentationException() {
    }
}
