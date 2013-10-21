package eu.europeana.cloud.service.mcs.rest.exceptionmappers;


import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import eu.europeana.cloud.service.mcs.exception.ProviderHasRecordsException;

@Provider
public class ProviderHasRecordsExceptionMapper implements ExceptionMapper<ProviderHasRecordsException> {

    @Override
    public Response toResponse(ProviderHasRecordsException exception) {
        return Response.status(Response.Status.METHOD_NOT_ALLOWED).entity(new ErrorInfo(exception)).build();
    }
}
