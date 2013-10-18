package eu.europeana.cloud.service.mcs.exception;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

/**
 * ProviderNotExistsException
 */
public class ProviderHasDataSetsException extends WebApplicationException {

    public ProviderHasDataSetsException() {
        super(Response.Status.METHOD_NOT_ALLOWED);
    }
}