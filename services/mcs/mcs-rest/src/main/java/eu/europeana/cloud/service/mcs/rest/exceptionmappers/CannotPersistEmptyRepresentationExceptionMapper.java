package eu.europeana.cloud.service.mcs.rest.exceptionmappers;

import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import eu.europeana.cloud.service.mcs.exception.CannotPersistEmptyRepresentationException;

/**
 * Maps {@link CannotPersistEmptyRepresentationException} to {@link javax.ws.rs.core.Response}.
 * 
 * @author marcinm@man.poznan.pl
 * @see ExceptionMapper
 */
@Provider
public class CannotPersistEmptyRepresentationExceptionMapper extends UnitedExceptionMapper implements
        ExceptionMapper<CannotPersistEmptyRepresentationException> {
}
