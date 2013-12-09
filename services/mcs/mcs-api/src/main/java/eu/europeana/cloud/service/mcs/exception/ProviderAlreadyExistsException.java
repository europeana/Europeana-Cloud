package eu.europeana.cloud.service.mcs.exception;

/**
 * Thrown whene there is attempt to create a provider that already exists.
 */
public class ProviderAlreadyExistsException extends Exception {

    public ProviderAlreadyExistsException() {
    }


    public ProviderAlreadyExistsException(String message) {
        super(message);
    }
}
