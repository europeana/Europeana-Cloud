package eu.europeana.cloud.service.aas.rest.exception;

import eu.europeana.cloud.service.aas.authentication.exception.UserExistsException;

import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

/**
 */
@Provider
public class UserExistsExceptionMapper extends AASExceptionMapper implements
        ExceptionMapper<UserExistsException> {
}
