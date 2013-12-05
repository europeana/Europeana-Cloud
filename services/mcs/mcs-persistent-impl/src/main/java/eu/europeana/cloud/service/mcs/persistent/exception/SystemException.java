package eu.europeana.cloud.service.mcs.persistent.exception;

/**
 * Exception that might be thrown by any service implementing persistent MCS service, indicating some problem with
 * external system (e.g. Casssandra or Swift connection problem).
 */
public class SystemException extends RuntimeException {

    public SystemException() {
    }


    public SystemException(Throwable cause) {
        super(cause);
    }


    public SystemException(String message) {
        super(message);
    }
}
