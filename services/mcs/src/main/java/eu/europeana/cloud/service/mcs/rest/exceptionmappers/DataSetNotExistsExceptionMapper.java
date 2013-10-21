package eu.europeana.cloud.service.mcs.rest.exceptionmappers;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;

@Provider
public class DataSetNotExistsExceptionMapper implements ExceptionMapper<DataSetNotExistsException> {

    @Override
    public Response toResponse(DataSetNotExistsException exception) {
        return Response.status(Response.Status.NOT_FOUND).entity(new ErrorInfo(exception)).build();
    }
}
