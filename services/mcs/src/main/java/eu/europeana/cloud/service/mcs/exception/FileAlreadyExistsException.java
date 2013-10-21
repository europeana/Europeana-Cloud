package eu.europeana.cloud.service.mcs.exception;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

/**
 * RecordNotExistsException
 */
public class FileAlreadyExistsException extends RuntimeException {

    public FileAlreadyExistsException() {
    }


    public FileAlreadyExistsException(String message) {
        super(message);
    }
}
