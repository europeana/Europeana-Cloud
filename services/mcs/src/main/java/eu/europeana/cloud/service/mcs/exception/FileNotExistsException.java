package eu.europeana.cloud.service.mcs.exception;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

/**
 * RecordNotExistsException
 */
public class FileNotExistsException extends RuntimeException {

    public FileNotExistsException() {
    }


    public FileNotExistsException(String message) {
        super(message);
    }
}
