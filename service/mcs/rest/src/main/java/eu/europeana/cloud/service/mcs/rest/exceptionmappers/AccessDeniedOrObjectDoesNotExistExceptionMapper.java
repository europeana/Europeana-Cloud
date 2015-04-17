package eu.europeana.cloud.service.mcs.rest.exceptionmappers;

import eu.europeana.cloud.service.mcs.exception.AccessDeniedOrObjectDoesNotExistException;

import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

/**
 * Maps {@link AccessDeniedOrObjectDoesNotExistExceptionMapper} to {@link javax.ws.rs.core.Response}.
 * 
 * @see ExceptionMapper
 */
@Provider
public class AccessDeniedOrObjectDoesNotExistExceptionMapper extends UnitedExceptionMapper implements
        ExceptionMapper<AccessDeniedOrObjectDoesNotExistException> {
}
