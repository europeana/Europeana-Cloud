package eu.europeana.cloud.service.mcs.rest.exceptionmappers;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

/**
 *
 */
@Provider
public class WebApplicationExceptionMapper extends UnitedExceptionMapper implements
        ExceptionMapper<WebApplicationException> {

}
