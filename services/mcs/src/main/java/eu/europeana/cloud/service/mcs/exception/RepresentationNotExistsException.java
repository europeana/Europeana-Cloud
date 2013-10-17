package eu.europeana.cloud.service.mcs.exception;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

/**
 * RecordNotExistsException
 */
public class RepresentationNotExistsException extends WebApplicationException {

    public RepresentationNotExistsException() {
        super(Response.Status.NOT_FOUND);
    }
}
