package eu.europeana.cloud.service.dps.services.submitters;

public class TaskSubmitException extends RuntimeException {

    public TaskSubmitException(String message) {
        super(message);
    }

    public TaskSubmitException(String message, Throwable cause) {
        super(message, cause);
    }
}
