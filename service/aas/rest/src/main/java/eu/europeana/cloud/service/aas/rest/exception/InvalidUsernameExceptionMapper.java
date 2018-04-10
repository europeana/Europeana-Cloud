package eu.europeana.cloud.service.aas.rest.exception;

import eu.europeana.cloud.service.aas.authentication.exception.InvalidUsernameException;

import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

/**
 * InvalidUsernameExceptionMapper exception mapper
 */
@Provider
public class InvalidUsernameExceptionMapper extends AASExceptionMapper implements
        ExceptionMapper<InvalidUsernameException> {

}
