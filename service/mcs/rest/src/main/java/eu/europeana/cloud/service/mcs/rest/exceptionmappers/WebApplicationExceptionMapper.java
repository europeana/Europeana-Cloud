package eu.europeana.cloud.service.mcs.rest.exceptionmappers;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

/**
 * Maps {@link WebApplicationExceptionMapper} to {@link javax.ws.rs.core.Response}.
 * 
 * @author marcinm@man.poznan.pl
 * @see javax.ws.rs.ext.ExceptionMapper
 */
@Provider
public class WebApplicationExceptionMapper extends UnitedExceptionMapper implements
        ExceptionMapper<WebApplicationException> {

}
