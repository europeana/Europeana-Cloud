package eu.europeana.cloud.service.mcs.rest.exceptionmappers;


import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import eu.europeana.cloud.service.mcs.exception.ProviderHasDataSetsException;

@Provider
public class ProviderHasDataSetsExceptionMapper implements ExceptionMapper<ProviderHasDataSetsException> {

    @Override
    public Response toResponse(ProviderHasDataSetsException exception) {
        return Response.status(Response.Status.METHOD_NOT_ALLOWED).entity(new ErrorInfo(exception)).build();
    }
}
