package eu.europeana.cloud.service.uis.exception;

import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
/**
 * DatabaseConnection exception mapper
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * @since Dec 17, 2013
 */
@Provider
public class DatabaseConnectionExceptionMapper extends UISExceptionMapper implements
		ExceptionMapper<DatabaseConnectionException> {

}
