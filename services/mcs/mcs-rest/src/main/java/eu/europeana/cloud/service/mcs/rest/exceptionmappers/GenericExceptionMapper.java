package eu.europeana.cloud.service.mcs.rest.exceptionmappers;

import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class GenericExceptionMapper extends UnitedExceptionMapper implements ExceptionMapper<RuntimeException> {
}
