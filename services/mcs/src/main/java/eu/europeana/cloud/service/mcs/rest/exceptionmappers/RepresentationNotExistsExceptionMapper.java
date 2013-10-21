package eu.europeana.cloud.service.mcs.rest.exceptionmappers;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;

@Provider
public class RepresentationNotExistsExceptionMapper implements ExceptionMapper<RepresentationNotExistsException> {

    @Override
    public Response toResponse(RepresentationNotExistsException exception) {
        return Response.status(Response.Status.NOT_FOUND).entity(new ErrorInfo(exception)).build();
    }
}
