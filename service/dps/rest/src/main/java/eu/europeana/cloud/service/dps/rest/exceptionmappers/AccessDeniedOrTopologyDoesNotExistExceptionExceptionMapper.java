package eu.europeana.cloud.service.dps.rest.exceptionmappers;

import eu.europeana.cloud.service.dps.exception.AccessDeniedOrTopologyDoesNotExistException;

import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class AccessDeniedOrTopologyDoesNotExistExceptionExceptionMapper extends UnitedExceptionMapper implements
        ExceptionMapper<AccessDeniedOrTopologyDoesNotExistException> {

}
