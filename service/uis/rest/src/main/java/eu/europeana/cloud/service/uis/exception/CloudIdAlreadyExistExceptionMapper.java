package eu.europeana.cloud.service.uis.exception;

import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

/**
 * CloudIdAlreadyExist exception mapper.
 * 
 */
@Provider
public class CloudIdAlreadyExistExceptionMapper extends UISExceptionMapper
	implements ExceptionMapper<CloudIdAlreadyExistException> {

}
