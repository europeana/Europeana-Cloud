package eu.europeana.cloud.service.mcs.exception;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

/**
 * RecordNotExistsException
 */
public class VersionNotExistsException extends RuntimeException {

    public VersionNotExistsException() {
    }


    public VersionNotExistsException(String message) {
        super(message);
    }
}
