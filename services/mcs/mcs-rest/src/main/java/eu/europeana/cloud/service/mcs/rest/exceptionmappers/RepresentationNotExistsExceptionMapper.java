package eu.europeana.cloud.service.mcs.rest.exceptionmappers;

import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;

@Provider
public class RepresentationNotExistsExceptionMapper extends UnitedExceptionMapper implements
        ExceptionMapper<RepresentationNotExistsException> {
}
