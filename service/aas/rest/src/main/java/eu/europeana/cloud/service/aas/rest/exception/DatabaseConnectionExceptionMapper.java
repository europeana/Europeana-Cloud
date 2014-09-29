package eu.europeana.cloud.service.aas.rest.exception;

import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import eu.europeana.cloud.service.aas.authentication.exception.DatabaseConnectionException;

/**
 * DatabaseConnectionExceptionMapper exception mapper
 */
@Provider
public class DatabaseConnectionExceptionMapper extends AASExceptionMapper implements
        ExceptionMapper<DatabaseConnectionException> {

}
