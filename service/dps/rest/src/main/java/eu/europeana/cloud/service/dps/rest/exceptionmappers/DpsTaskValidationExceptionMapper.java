package eu.europeana.cloud.service.dps.rest.exceptionmappers;

import eu.europeana.cloud.service.dps.exception.DpsTaskValidationException;

import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class DpsTaskValidationExceptionMapper extends UnitedExceptionMapper implements
        ExceptionMapper<DpsTaskValidationException> {
}
