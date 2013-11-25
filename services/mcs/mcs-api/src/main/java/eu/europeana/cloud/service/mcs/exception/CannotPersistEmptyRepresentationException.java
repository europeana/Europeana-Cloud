package eu.europeana.cloud.service.mcs.exception;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

/**
 * RecordNotExistsException
 */
public class CannotPersistEmptyRepresentationException extends RuntimeException {

    public CannotPersistEmptyRepresentationException(String message) {
        super(message);
    }


    public CannotPersistEmptyRepresentationException() {
    }
}
