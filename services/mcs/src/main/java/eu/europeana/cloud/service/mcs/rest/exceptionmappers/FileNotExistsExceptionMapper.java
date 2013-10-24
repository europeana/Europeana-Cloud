package eu.europeana.cloud.service.mcs.rest.exceptionmappers;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;

@Provider
public class FileNotExistsExceptionMapper extends UnitedExceptionMapper
        implements ExceptionMapper<FileNotExistsException> {
}
