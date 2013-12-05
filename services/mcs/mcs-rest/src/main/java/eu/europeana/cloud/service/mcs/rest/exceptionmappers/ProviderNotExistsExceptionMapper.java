package eu.europeana.cloud.service.mcs.rest.exceptionmappers;

import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;

@Provider
public class ProviderNotExistsExceptionMapper extends UnitedExceptionMapper implements
        ExceptionMapper<ProviderNotExistsException> {
}
