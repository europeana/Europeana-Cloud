package eu.europeana.cloud.service.mcs.exception;

/**
 * ProviderNotExistsException
 */
public class ProviderHasDataSetsException extends RuntimeException {

    public ProviderHasDataSetsException() {
    }


    public ProviderHasDataSetsException(String message) {
        super(message);
    }
}