package eu.europeana.cloud.service.mcs.rest.exceptionmappers;

import org.springframework.stereotype.Component;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;

@Provider
public class CannotModifyPersistentRepresentationExceptionMapper extends UnitedExceptionMapper
        implements ExceptionMapper<CannotModifyPersistentRepresentationException> {
}
