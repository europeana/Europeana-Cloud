package eu.europeana.cloud.service.mcs.exception;

/**
 * Thrown when there is attempt to get data provider which does not exist.
 */
public class ProviderNotExistsException extends Exception {

    public ProviderNotExistsException() {
    }


    public ProviderNotExistsException(String message) {
        super(message);
    }
}