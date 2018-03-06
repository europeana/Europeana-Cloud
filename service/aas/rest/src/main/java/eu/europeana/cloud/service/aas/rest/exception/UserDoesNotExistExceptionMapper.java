package eu.europeana.cloud.service.aas.rest.exception;

import eu.europeana.cloud.service.aas.authentication.exception.UserDoesNotExistException;

import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

/**
 * UserDoesNotExistExceptionMapper exception mapper
 */
@Provider
public class UserDoesNotExistExceptionMapper extends AASExceptionMapper implements
        ExceptionMapper<UserDoesNotExistException> {

}
