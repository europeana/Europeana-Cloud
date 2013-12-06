package eu.europeana.cloud.service.mcs.exception;

/**
 * Thrown when using cloud id which is not registered in Unique Identifier Service (UID).
 */
public class RecordNotExistsException extends RuntimeException {

    public RecordNotExistsException() {
    }


    public RecordNotExistsException(String recordId) {
        super("There is no record with provided global id: " + recordId);
    }
}
