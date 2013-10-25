package eu.europeana.cloud.service.mcs.rest.exceptionmappers;

import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;

@Provider
public class CannotModifyPersistentRepresentationExceptionMapper extends UnitedExceptionMapper
        implements ExceptionMapper<CannotModifyPersistentRepresentationException> {
}
