package eu.europeana.cloud.service.mcs.rest.exceptionmappers;


import eu.europeana.cloud.service.mcs.exception.RevisionIsNotValidException;

import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

/**
 * Maps {@link RevisionIsNotValidExceptionMapper} to {@link javax.ws.rs.core.Response}.
 * 
 * @see ExceptionMapper
 */
@Provider
public class RevisionIsNotValidExceptionMapper extends UnitedExceptionMapper implements
        ExceptionMapper<RevisionIsNotValidException> {
}
