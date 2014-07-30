package eu.europeana.cloud.service.aas.exception;

/**
 * Thrown in case the current user doesn't have the required rights to perform an action.
 */
public class AccessDeniedException extends AASException {
	
    /**
     * Constructs a AccessDeniedException with no specified detail message.
     */
    public AccessDeniedException() {
        super();
    }

    /**
     * Constructs an AccessDeniedException with the specified detail message.
     * 
     * @param message
     *            the detail message
     */
    public AccessDeniedException(String message) {
        super(message);
    }

    /**
     * Constructs an AccessDeniedException with the specified detail message.
     * 
     * @param message the detail message
     * @param e exception
     */
    public AccessDeniedException(String message, Exception e) {
        super(message, e);
    }
}
