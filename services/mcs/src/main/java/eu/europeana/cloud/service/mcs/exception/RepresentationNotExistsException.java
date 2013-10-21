package eu.europeana.cloud.service.mcs.exception;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

/**
 * RecordNotExistsException
 */
public class RepresentationNotExistsException extends RuntimeException {

    public RepresentationNotExistsException() {
    }


    public RepresentationNotExistsException(String message) {
        super(message);
    }
}
