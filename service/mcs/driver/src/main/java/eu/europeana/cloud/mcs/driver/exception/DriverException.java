package eu.europeana.cloud.mcs.driver.exception;

/**
 * Thrown when an internal error happened in Metadata and Content Service.
 */
public class DriverException extends RuntimeException {

    /**
     * Constructs a ServiceInternalErrorException with the specified detail
     * message.
     *
     * @param message the detail message
     */
    public DriverException(String message) {
        super(message);
    }

    /**
     * Constructs a ServiceInternalErrorException with no specified detail
     * message.
     */
    public DriverException() {
    }

    /**
     * Constructs a ServiceInternalErrorException with the specified detail
     * message and inner Exception.
     *
     * @param message the detail message
     * @param e inner Exception
     */
    public DriverException(String message, Exception e) {
        super(message, e);
    }
}
