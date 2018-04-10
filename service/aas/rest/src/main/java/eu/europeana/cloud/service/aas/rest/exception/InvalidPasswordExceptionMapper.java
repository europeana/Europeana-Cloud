package eu.europeana.cloud.service.aas.rest.exception;

import eu.europeana.cloud.service.aas.authentication.exception.InvalidPasswordException;

import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

/**
 * InvalidPasswordExceptionMapper exception mapper
 */
@Provider
public class InvalidPasswordExceptionMapper extends AASExceptionMapper implements
        ExceptionMapper<InvalidPasswordException> {

}
