package eu.europeana.cloud.service.mcs.exception;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

/**
 * RecordNotExistsException
 */
public class RecordNotExistsException extends WebApplicationException {

    public RecordNotExistsException() {
        super(Response.Status.NOT_FOUND);
    }
}
