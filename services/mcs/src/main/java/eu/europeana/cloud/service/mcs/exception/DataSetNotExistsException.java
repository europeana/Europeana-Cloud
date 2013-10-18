package eu.europeana.cloud.service.mcs.exception;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

/**
 * DataSetNotExistsException
 */
public class DataSetNotExistsException extends WebApplicationException {

    public DataSetNotExistsException() {
        super(Response.Status.NOT_FOUND);
    }
}