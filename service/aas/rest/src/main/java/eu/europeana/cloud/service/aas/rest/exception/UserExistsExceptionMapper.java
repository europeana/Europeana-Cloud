package eu.europeana.cloud.service.aas.rest.exception;

import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import eu.europeana.cloud.service.aas.authentication.exception.UserExistsException;

/**
 */
@Provider
public class UserExistsExceptionMapper extends AASExceptionMapper implements
        ExceptionMapper<UserExistsException> {
}
