package eu.europeana.cloud.service.aas.rest.exception;

import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import eu.europeana.cloud.service.aas.authentication.exception.InvalidUsernameException;

/**
 * InvalidUsernameExceptionMapper exception mapper
 */
@Provider
public class InvalidUsernameExceptionMapper extends AASExceptionMapper implements
        ExceptionMapper<InvalidUsernameException> {

}
