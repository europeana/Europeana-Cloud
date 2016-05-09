package eu.europeana.cloud.service.dps.exception;


public class AccessDeniedOrTopologyDoesNotExistException extends  DpsException {

    /**
     * Constructs an AccessDeniedOrTopologyDoesNotExistException with no specified detail message.
     */
    public AccessDeniedOrTopologyDoesNotExistException() {
    }


    /**
     * Constructs an AccessDeniedOrTopologyDoesNotExistException with the specified detail message.
     *
     * @param message the detail message
     */
    public AccessDeniedOrTopologyDoesNotExistException(String message) {
        super(message);
    }
}
