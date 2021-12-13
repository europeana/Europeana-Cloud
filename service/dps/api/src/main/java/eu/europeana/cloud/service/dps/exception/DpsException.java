package eu.europeana.cloud.service.dps.exception;

/**
 * Base class for all exceptions that may be thrown from the DPS.
 */
public class DpsException extends Exception {
	
     /**
     * Constructs a DpsException with no specified detail message.
     */
    public DpsException() {
        super();
    }

    /**
     * Constructs a DpsException with the specified detail message.
     * 
     * @param message the detail message
     */
    public DpsException(String message) {
        super(message);
    }

     /**
     * Constructs a DpsException with the specified Throwable.
     * 
     * @param cause  the cause
     */
    public DpsException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a DpsException with the specified detail message.
     *
     * @param message the detailed message
     * @param cause throwable object
     */
    public DpsException(String message, Throwable cause) {
        super(message, cause);
    }
}
