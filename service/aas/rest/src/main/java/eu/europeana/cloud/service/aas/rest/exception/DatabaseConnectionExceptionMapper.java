package eu.europeana.cloud.service.aas.rest.exception;

import eu.europeana.cloud.service.aas.authentication.exception.DatabaseConnectionException;

import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

/**
 * DatabaseConnectionExceptionMapper exception mapper
 */
@Provider
public class DatabaseConnectionExceptionMapper extends AASExceptionMapper implements
        ExceptionMapper<DatabaseConnectionException> {

}
