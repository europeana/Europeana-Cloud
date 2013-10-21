package eu.europeana.cloud.service.mcs.rest.exceptionmappers;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;

@Provider
public class FileAlreadyExistsExceptionMapper implements ExceptionMapper<FileAlreadyExistsException> {

    @Override
    public Response toResponse(FileAlreadyExistsException exception) {
        return Response.status(Response.Status.CONFLICT).entity(new ErrorInfo(exception)).build();
    }
}
