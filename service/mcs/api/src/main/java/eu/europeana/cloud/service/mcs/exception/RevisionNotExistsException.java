package eu.europeana.cloud.service.mcs.exception;

/**
 * Thrown when there is attempt to get non existing Revision from a specific representation version.
 */
public class RevisionNotExistsException extends MCSException {

    /**
     * Constructs a RevisionNotExistsException with no specified detail message.
     */
    public RevisionNotExistsException() {
    }


    /**
     * Constructs a RevisionNotExistsException with the specified detail message.
     *
     * @param message
     *            the detail message
     */
    public RevisionNotExistsException(String message) {
        super(message);
    }
}
