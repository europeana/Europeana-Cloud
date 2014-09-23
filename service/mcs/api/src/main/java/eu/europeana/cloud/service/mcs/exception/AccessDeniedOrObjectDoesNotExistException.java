package eu.europeana.cloud.service.mcs.exception;

/**
 * Thrown when there is attempt to access a resource without the proper permissions.
 * 
 * Also thrown when the resource does not exist at all. 
 */
public class AccessDeniedOrObjectDoesNotExistException extends MCSException {

    /**
     * Constructs an AccessDeniedOrObjectDoesNotExistException with no specified detail message.
     */
    public AccessDeniedOrObjectDoesNotExistException() {
    }


    /**
     * Constructs an AccessDeniedOrObjectDoesNotExistException with the specified detail message.
     * 
     * @param message the detail message
     */
    public AccessDeniedOrObjectDoesNotExistException(String message) {
        super(message);
    }
}
