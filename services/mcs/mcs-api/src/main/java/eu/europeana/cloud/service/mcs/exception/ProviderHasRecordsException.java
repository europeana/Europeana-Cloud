package eu.europeana.cloud.service.mcs.exception;

/**
 * Thrown if there is attempt to delete a data provider which has created some record's representation versions. In
 * order to delete data provider, all his representation versions must be implicitly removed first.
 */
public class ProviderHasRecordsException extends RuntimeException {

    public ProviderHasRecordsException() {
    }


    public ProviderHasRecordsException(String message) {
        super(message);
    }
}