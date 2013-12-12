package eu.europeana.cloud.service.mcs.rest.exceptionmappers;

import eu.europeana.cloud.service.mcs.exception.ProviderAlreadyExistsException;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class ProviderAlreadyExistsExceptionMapper extends UnitedExceptionMapper implements
        ExceptionMapper<ProviderAlreadyExistsException> {
}
