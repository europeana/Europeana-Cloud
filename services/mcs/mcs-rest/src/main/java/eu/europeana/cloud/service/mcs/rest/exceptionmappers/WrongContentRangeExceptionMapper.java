package eu.europeana.cloud.service.mcs.rest.exceptionmappers;

import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import eu.europeana.cloud.service.mcs.exception.WrongContentRangeException;

/**
 * Maps {@link WrongContentRangeExceptionMapper} to {@link javax.ws.rs.core.Response}.
 * 
 * @author marcinm@man.poznan.pl
 * @see javax.ws.rs.ext.ExceptionMapper
 */
@Provider
public class WrongContentRangeExceptionMapper extends UnitedExceptionMapper implements
        ExceptionMapper<WrongContentRangeException> {
}
