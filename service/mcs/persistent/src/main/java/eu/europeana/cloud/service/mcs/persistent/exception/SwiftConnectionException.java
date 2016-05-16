package eu.europeana.cloud.service.mcs.persistent.exception;

/**
 * Exception to be thrown if there are problem with connection to Open Stack
 * Swift Proxy.
 */
public class SwiftConnectionException extends RuntimeException {

    /**
     * Constructs a SwiftConnectionException.
     * 
     */
    public SwiftConnectionException() {
        super("Cannot establish connection to proxy.");
    }
}
