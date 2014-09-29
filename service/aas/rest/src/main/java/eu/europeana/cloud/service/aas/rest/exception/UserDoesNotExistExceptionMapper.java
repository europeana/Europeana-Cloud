package eu.europeana.cloud.service.aas.rest.exception;

import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import eu.europeana.cloud.service.aas.authentication.exception.UserDoesNotExistException;

/**
 * UserDoesNotExistExceptionMapper exception mapper
 */
@Provider
public class UserDoesNotExistExceptionMapper extends AASExceptionMapper implements
        ExceptionMapper<UserDoesNotExistException> {

}
