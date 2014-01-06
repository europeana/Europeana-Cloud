package eu.europeana.cloud.service.mcs.rest.exceptionmappers;

import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;

/**
 * ProviderDoesNotExist exception mapper
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * @since Dec 17, 2013
 */
@Provider
public class ProviderDoesNotExistExceptionMapper extends UnitedExceptionMapper implements
		ExceptionMapper<ProviderDoesNotExistException> {

}
