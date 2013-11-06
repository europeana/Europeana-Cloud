package eu.europeana.cloud.service.mcs.exception;

/**
 * ProviderNotExistsException
 */
public class ProviderAlreadyExistsException extends RuntimeException {

    public ProviderAlreadyExistsException() {
    }


    public ProviderAlreadyExistsException(String message) {
        super(message);
    }
}