package eu.europeana.cloud.service.dps.storm.exception;

import eu.europeana.cloud.service.dps.exception.DpsException;

/**
 * Created by Tarek on 4/12/2018.
 */
public class ObjectAlreadyBeenInitializedException extends DpsException {
    public ObjectAlreadyBeenInitializedException(String message) {
        super(message);
    }

    public ObjectAlreadyBeenInitializedException() {
    }
}
