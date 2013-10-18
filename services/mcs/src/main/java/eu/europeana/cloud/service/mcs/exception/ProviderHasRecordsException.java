package eu.europeana.cloud.service.mcs.exception;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

/**
 * ProviderNotExistsException
 */
public class ProviderHasRecordsException extends WebApplicationException {

    public ProviderHasRecordsException() {
        super(Response.Status.METHOD_NOT_ALLOWED);
    }
}