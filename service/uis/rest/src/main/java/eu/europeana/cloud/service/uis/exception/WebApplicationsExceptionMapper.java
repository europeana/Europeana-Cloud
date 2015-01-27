package eu.europeana.cloud.service.uis.exception;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.ext.ExceptionMapper;

public class WebApplicationsExceptionMapper extends UISExceptionMapper implements
        ExceptionMapper<WebApplicationException> {

}
