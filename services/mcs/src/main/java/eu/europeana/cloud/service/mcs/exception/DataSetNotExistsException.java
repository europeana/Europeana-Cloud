package eu.europeana.cloud.service.mcs.exception;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

/**
 * DataSetNotExistsException
 */
public class DataSetNotExistsException extends RuntimeException {

    public DataSetNotExistsException() {
    }


    public DataSetNotExistsException(String message) {
        super(message);
    }
}