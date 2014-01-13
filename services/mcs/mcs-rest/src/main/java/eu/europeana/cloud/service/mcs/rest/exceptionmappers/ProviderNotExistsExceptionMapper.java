package eu.europeana.cloud.service.mcs.rest.exceptionmappers;

import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class ProviderNotExistsExceptionMapper extends UnitedExceptionMapper implements
        ExceptionMapper<ProviderNotExistsException> {
}
