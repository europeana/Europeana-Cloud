package eu.europeana.cloud.service.dps.rest.exceptionmappers;

import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import eu.europeana.cloud.service.dps.exception.TopologyAlreadyExistsException;

/**
 * Maps {@link TopologyAlreadyExistsExceptionMapper} to {@link javax.ws.rs.core.Response}.
 * 
 */
@Provider
public class TopologyAlreadyExistsExceptionMapper extends UnitedExceptionMapper implements
        ExceptionMapper<TopologyAlreadyExistsException> {
}
