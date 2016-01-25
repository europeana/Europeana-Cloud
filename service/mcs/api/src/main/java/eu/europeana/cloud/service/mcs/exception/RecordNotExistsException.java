package eu.europeana.cloud.service.mcs.exception;

/**
 * Thrown when using cloud id which is not registered in Unique Identifier Service (UID).
 */
public class RecordNotExistsException extends MCSException {

    /**
     * Constructs a RecordNotExistsException with no specified detail message.
     */
    public RecordNotExistsException() {
    }


    /**
     * Constructs a RecordNotExistsException with the specified record identifier.
     * 
     * @param recordId
     *            the record identifier
     */
    public RecordNotExistsException(String recordId) {
        super("There is no record with provided global id: " + recordId);
    }

    /**
     * Constructs a RecordNotExistsException with the specified local identifier and provider name.
     *
     * @param localId
     *            the local identifier
     * @param providerId provider name
     */
    public RecordNotExistsException(String localId, String providerId) {
        super("There is no record with provided local id: " + localId + " and provider name " + providerId);
    }

}
