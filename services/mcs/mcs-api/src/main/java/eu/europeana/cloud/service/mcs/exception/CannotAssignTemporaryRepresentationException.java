package eu.europeana.cloud.service.mcs.exception;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

/**
 * RecordNotExistsException
 */
public class CannotAssignTemporaryRepresentationException extends RuntimeException {

    public CannotAssignTemporaryRepresentationException(String message) {
        super(message);
    }


    public CannotAssignTemporaryRepresentationException() {
    }
}
