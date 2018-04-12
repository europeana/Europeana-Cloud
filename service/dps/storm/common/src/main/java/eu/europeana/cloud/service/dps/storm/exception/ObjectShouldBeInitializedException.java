package eu.europeana.cloud.service.dps.storm.exception;

import eu.europeana.cloud.service.dps.exception.DpsException;

/**
 * Created by Tarek on 4/12/2018.
 */
public class ObjectShouldBeInitializedException extends DpsException {
    public ObjectShouldBeInitializedException(String message) {
        super(message);
    }

    public ObjectShouldBeInitializedException() {
    }
}
