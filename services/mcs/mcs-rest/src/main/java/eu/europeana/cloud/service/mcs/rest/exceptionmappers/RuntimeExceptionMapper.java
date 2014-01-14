package eu.europeana.cloud.service.mcs.rest.exceptionmappers;

import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

/**
 * Maps all unhandled runtime exceptions. The http response will contain {@link McsErrorCode#OTHER} error code and
 * exception message as details.
 * 
 */
@Provider
public class RuntimeExceptionMapper extends UnitedExceptionMapper implements ExceptionMapper<RuntimeException> {
}
