package eu.europeana.cloud.service.mcs.exception;

/**
 * ProviderNotExistsException
 */
public class WrongContentRangeException extends RuntimeException {

    public WrongContentRangeException() {
    }


    public WrongContentRangeException(String message) {
        super(message);
    }
}