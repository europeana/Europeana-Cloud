package eu.europeana.cloud.contentserviceapi.exception;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

/**
 * RecordNotExistsException
 */
public class RepresentationAlreadyPersistentException extends WebApplicationException {

    public RepresentationAlreadyPersistentException() {
        super(Response.Status.METHOD_NOT_ALLOWED);
    }
}
