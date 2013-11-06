package eu.europeana.cloud.service.mcs.exception;

/**
 * RecordNotExistsException
 */
public class RecordNotExistsException extends RuntimeException {

    public RecordNotExistsException() {
    }


    public RecordNotExistsException(String recordId) {
        super("There is no record with provided global id: " + recordId);
    }
}
