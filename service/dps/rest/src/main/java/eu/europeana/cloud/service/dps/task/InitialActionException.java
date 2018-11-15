package eu.europeana.cloud.service.dps.task;

/**
 * Created by pwozniak on 10/4/18
 */
public class InitialActionException extends Exception {

    public InitialActionException(String message, Throwable cause) {
        super(message, cause);
    }

    public InitialActionException(String message) {
        super(message);

    }
}
