package eu.europeana.cloud.service.dls.solr.exception;

/**
 * Exception indicating some problem with external system (e.g. Solr connection problem).
 */
public class SystemException extends RuntimeException {

    /**
     * Constructs a SystemException with no specified detail message.
     */
    public SystemException() {
    }


    /**
     * Constructs a SystemException with the specified Throwable.
     * 
     * @param cause
     *            the cause
     */
    public SystemException(Throwable cause) {
        super(cause);
    }


    /**
     * Constructs a SystemException with the specified detail message.
     * 
     * @param message
     *            the detail message
     * @param cause
     *            the cause
     */
    public SystemException(String message) {
        super(message);
    }


    /**
     * Constructs a SystemException with the specified detail message.
     * 
     * @param message
     *            the detail message
     */
    public SystemException(String message, Throwable cause) {
        super(message, cause);
    }
}
