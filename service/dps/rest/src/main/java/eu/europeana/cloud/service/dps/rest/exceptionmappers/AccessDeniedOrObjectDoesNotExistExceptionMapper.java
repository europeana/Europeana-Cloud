package eu.europeana.cloud.service.dps.rest.exceptionmappers;

import eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException;

import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class AccessDeniedOrObjectDoesNotExistExceptionMapper extends UnitedExceptionMapper implements
        ExceptionMapper<AccessDeniedOrObjectDoesNotExistException> {
}
