package eu.europeana.cloud.service.mcs.exception;

public class BaseMCSException extends Exception {
     /**
     * Constructs a BaseMCSException with no specified detail message.
     */
    public BaseMCSException() {
        super();
    }

    /**
     * Constructs a BaseMCSException with the specified detail message.
     * 
     * @param message
     *            the detail message
     */
    public BaseMCSException(String message) {
        super(message);
    }

    /**
     * Constructs a BaseMCSException with the specified detail message.
     * 
     * @param message
     *            the detail message
     * @param e
     *            exception
     */
    public BaseMCSException(String message, Exception e) {
        super(message, e);
    }
    
     /**
     * Constructs a BaseMCSException with the specified Throwable.
     * 
     * @param cause
     *            the cause
     */
    public BaseMCSException(Throwable cause) {
        super(cause);
    }
    
}
