package eu.europeana.cloud.service.mcs.exception;

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
     * @param message
     *            the detail message
     */
    public MCSException(String message) {
        super(message);
    }

    /**
     * Constructs a MCSException with the specified detail message.
     * 
     * @param message
     *            the detail message
     * @param e
     *            exception
     */
    public MCSException(String message, Exception e) {
        super(message, e);
    }
    
     /**
     * Constructs a MCSException with the specified Throwable.
     * 
     * @param cause
     *            the cause
     */
    public MCSException(Throwable cause) {
        super(cause);
    }
    
}
