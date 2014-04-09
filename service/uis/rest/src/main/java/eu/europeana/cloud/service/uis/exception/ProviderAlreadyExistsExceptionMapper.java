package eu.europeana.cloud.service.uis.exception;

import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

/**
 * Provider Already Exists exception mapper
 * 
 * @author Yorgos.Mamakis@ europeana.eu
 */
@Provider
public class ProviderAlreadyExistsExceptionMapper extends UISExceptionMapper implements
        ExceptionMapper<ProviderAlreadyExistsException> {
}
