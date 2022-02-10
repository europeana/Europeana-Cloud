package eu.europeana.cloud.service.mcs.exception;

/**
 * Base class for all exceptions that may be thrown in MCS.
 */
public class MCSException extends Exception {
    /**
     * Constructs a MCSException with no specified detail message.
     */
    public MCSException() {
        super();
    }

    /**
     * Constructs a MCSException with the specified detail message.
     *
     * @param message the detail message
     */
    public MCSException(String message) {
        super(message);
    }

    /**
     * Constructs a MCSException with the specified Throwable.
     *
     * @param cause the cause
     */
    public MCSException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a MCSException with the specified detail message.
     *
     * @param message   the detail message
     * @param throwable cause exception or other throwable
     */
    public MCSException(String message, Throwable throwable) {
        super(message, throwable);
    }

}
