package eu.europeana.cloud.service.mcs.rest.exceptionmappers;

import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;

import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

/**
 * Maps {@link CannotModifyPersistentRepresentationExceptionMapper} to {@link javax.ws.rs.core.Response}.
 * 
 * @author marcinm@man.poznan.pl
 * @see javax.ws.rs.ext.ExceptionMapper
 */
@Provider
public class CannotModifyPersistentRepresentationExceptionMapper extends UnitedExceptionMapper implements
        ExceptionMapper<CannotModifyPersistentRepresentationException> {
}
