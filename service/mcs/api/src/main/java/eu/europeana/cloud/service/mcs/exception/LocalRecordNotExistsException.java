package eu.europeana.cloud.service.mcs.exception;

/**
 * Thrown when using local id which is not registered for provider id in Unique Identifier Service (UID).
 */
public class LocalRecordNotExistsException extends MCSException {
    /**
     * Constructs a LocalRecordNotExistsException with no specified detail message.
     */
    public LocalRecordNotExistsException() {
    }


    /**
     * Constructs a LocalRecordNotExistsException with the specified local record identifier and provider identifier.
     *
     * @param localId the record local identifier
     * @param providerId provider identifier
     *
     */
    public LocalRecordNotExistsException(String providerId, String localId) {
        super("There is no record with local id: " + localId + " for provider: " + providerId);
    }
}
