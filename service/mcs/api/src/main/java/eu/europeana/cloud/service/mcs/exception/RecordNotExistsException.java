package eu.europeana.cloud.service.mcs.exception;

/**
 * Thrown when using cloud id which is not registered in Unique Identifier Service (UID).
 */
public class RecordNotExistsException extends BaseMCSException {

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
}
