package eu.europeana.cloud.contentserviceapi.exception;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

/**
 * RecordNotExistsException
 */
public class FileAlreadyExistsException extends WebApplicationException {

    public FileAlreadyExistsException() {
        super(Response.Status.CONFLICT);
    }
}
