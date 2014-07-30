package eu.europeana.cloud.service.aas.exception;

public class AASException extends Exception {
	
     /**
     * Constructs an AASException with no specified detail message.
     */
    public AASException() {
        super();
    }

    /**
     * Constructs an AASException with the specified detail message.
     * 
     * @param message details
     */
    public AASException(String message) {
        super(message);
    }

    /**
     * Constructs an AASException with the specified detail message.
     * 
     * @param message details
     * @param e exception
     */
    public AASException(String message, Exception e) {
        super(message, e);
    }
    
     /**
     * Constructs an AASException with the specified Throwable.
     * 
     * @param cause the cause
     */
    public AASException(Throwable cause) {
        super(cause);
    }
    
}
