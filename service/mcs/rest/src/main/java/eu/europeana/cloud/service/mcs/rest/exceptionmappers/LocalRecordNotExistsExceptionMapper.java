package eu.europeana.cloud.service.mcs.rest.exceptionmappers;

import eu.europeana.cloud.service.mcs.exception.LocalRecordNotExistsException;

import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class LocalRecordNotExistsExceptionMapper extends UnitedExceptionMapper implements
        ExceptionMapper<LocalRecordNotExistsException> {
}
