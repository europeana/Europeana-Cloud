package eu.europeana.cloud.service.mcs.rest.exceptionmappers;

import org.springframework.stereotype.Component;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException;

@Provider
public class DataSetAlreadyExistsExceptionMapper implements ExceptionMapper<DataSetAlreadyExistsException> {

    @Override
    public Response toResponse(DataSetAlreadyExistsException exception) {
        return Response.status(Response.Status.CONFLICT).entity(new ErrorInfo(exception)).build();
    }
}
