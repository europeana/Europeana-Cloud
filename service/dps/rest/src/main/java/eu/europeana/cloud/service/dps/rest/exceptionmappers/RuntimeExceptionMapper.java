package eu.europeana.cloud.service.dps.rest.exceptionmappers;

import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class RuntimeExceptionMapper extends UnitedExceptionMapper implements ExceptionMapper<RuntimeException> {
}
