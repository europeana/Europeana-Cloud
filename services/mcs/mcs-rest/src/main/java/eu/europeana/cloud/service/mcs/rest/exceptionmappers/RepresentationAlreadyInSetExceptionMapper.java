package eu.europeana.cloud.service.mcs.rest.exceptionmappers;

import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import eu.europeana.cloud.service.mcs.exception.RepresentationAlreadyInSetException;

@Provider
public class RepresentationAlreadyInSetExceptionMapper extends UnitedExceptionMapper implements
        ExceptionMapper<RepresentationAlreadyInSetException> {
}
