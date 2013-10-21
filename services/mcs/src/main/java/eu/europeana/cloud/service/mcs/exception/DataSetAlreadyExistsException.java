package eu.europeana.cloud.service.mcs.exception;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

/**
 * DataSetAlreadyExistsException
 */
public class DataSetAlreadyExistsException extends RuntimeException {

    public DataSetAlreadyExistsException(String message) {
        super(message);
    }


    public DataSetAlreadyExistsException() {
    }
}