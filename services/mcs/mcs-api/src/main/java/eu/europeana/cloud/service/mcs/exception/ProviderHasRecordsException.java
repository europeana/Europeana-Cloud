package eu.europeana.cloud.service.mcs.exception;

/**
 * ProviderNotExistsException
 */
public class ProviderHasRecordsException extends RuntimeException {

    public ProviderHasRecordsException() {
    }


    public ProviderHasRecordsException(String message) {
        super(message);
    }
}