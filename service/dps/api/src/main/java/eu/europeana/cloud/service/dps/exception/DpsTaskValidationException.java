package eu.europeana.cloud.service.dps.exception;

public class DpsTaskValidationException extends DpsException {

    public DpsTaskValidationException() {
        super();
    }

    public DpsTaskValidationException(String message) {
        super(message);
    }

    public DpsTaskValidationException(String message,Throwable t) {
        super(message,t);
    }
}
