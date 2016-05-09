package eu.europeana.cloud.service.dps.rest.exceptionmappers;

import eu.europeana.cloud.service.dps.exception.AccessDeniedOrTopologyDoesNotExistException;
import eu.europeana.cloud.service.dps.service.utils.validation.DpsTaskValidationException;

import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class DpsTaskValidationExceptionMapper extends UnitedExceptionMapper implements
        ExceptionMapper<DpsTaskValidationException> {
}
