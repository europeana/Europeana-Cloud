package eu.europeana.cloud.service.mcs.exception;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

/**
 * ProviderNotExistsException
 */
public class ProviderHasDataSetsException extends RuntimeException {

    public ProviderHasDataSetsException() {
    }


    public ProviderHasDataSetsException(String message) {
        super(message);
    }
}