package eu.europeana.cloud.service.mcs.exception;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

/**
 * ProviderNotExistsException
 */
public class ProviderNotExistsException extends RuntimeException {

    public ProviderNotExistsException() {
    }


    public ProviderNotExistsException(String message) {
        super(message);
    }
}