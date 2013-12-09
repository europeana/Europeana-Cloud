package eu.europeana.cloud.service.mcs.exception;

/**
 * Thrown whren there was attempt to modify persistent representation version.
 */
public class CannotModifyPersistentRepresentationException extends Exception {

    public CannotModifyPersistentRepresentationException(String message) {
        super(message);
    }


    public CannotModifyPersistentRepresentationException() {
    }
}
