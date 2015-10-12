package eu.europeana.cloud.service.dps.storm.topologies.ic.converter.exceptions;

/**
 * Created by Tarek on 8/24/2015.
 */

public class ICSException extends Exception {
    /**
     * Constructs a ICSException with no specified detail message.
     */
    public ICSException() {
        super();
    }

    /**
     * Constructs a ICSException with the specified detail message.
     *
     * @param message
     *            the detail message
     */
    public ICSException(String message) {
        super(message);
    }

    /**
     * Constructs a ICSException with the specified detail message.
     *
     * @param message
     *            the detail message
     * @param e
     *            exception
     */
    public ICSException(String message, Exception e) {
        super(message, e);
    }

    /**
     * Constructs a ICSException with the specified Throwable.
     *
     * @param cause
     *            the cause
     */
    public ICSException(Throwable cause) {
        super(cause);
    }

}
