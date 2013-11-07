package eu.europeana.cloud.service.mcs.exception;

/**
 * ProviderNotExistsException
 */
public class ProviderNotExistsException extends RuntimeException {

    public ProviderNotExistsException() {
    }


    public ProviderNotExistsException(String message) {
        super(message);
    }
}