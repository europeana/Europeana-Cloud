package eu.europeana.cloud.service.mcs.exception;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

/**
 * RecordNotExistsException
 */
public class CannotModifyPersistentRepresentationException extends RuntimeException {

    public CannotModifyPersistentRepresentationException(String message) {
        super(message);
    }


    public CannotModifyPersistentRepresentationException() {
    }
}
