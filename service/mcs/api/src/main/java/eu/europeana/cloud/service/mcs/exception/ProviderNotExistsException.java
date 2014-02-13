package eu.europeana.cloud.service.mcs.exception;

/**
 * Thrown when there is attempt to get data provider which does not exist.
 */
public class ProviderNotExistsException extends MCSException {

    /**
     * Constructs a ProviderNotExistsException with no specified detail message.
     */
    public ProviderNotExistsException() {
    }


    /**
     * Constructs a ProviderNotExistsException with the specified detail message.
     * 
     * @param message
     *            the detail message
     */
    public ProviderNotExistsException(String message) {
        super(message);
    }
}