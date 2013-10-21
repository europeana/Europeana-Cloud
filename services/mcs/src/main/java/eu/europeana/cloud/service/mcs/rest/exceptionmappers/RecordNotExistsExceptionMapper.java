package eu.europeana.cloud.service.mcs.rest.exceptionmappers;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;

@Provider
public class RecordNotExistsExceptionMapper implements ExceptionMapper<RecordNotExistsException> {

    @Override
    public Response toResponse(RecordNotExistsException exception) {
        return Response.status(Response.Status.NOT_FOUND).entity(new ErrorInfo(exception)).build();
    }
}
