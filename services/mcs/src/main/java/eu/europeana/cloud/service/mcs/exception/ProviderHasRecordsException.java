package eu.europeana.cloud.service.mcs.exception;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

/**
 * ProviderNotExistsException
 */
public class ProviderHasRecordsException extends RuntimeException {

    public ProviderHasRecordsException() {
    }


    public ProviderHasRecordsException(String message) {
        super(message);
    }
}