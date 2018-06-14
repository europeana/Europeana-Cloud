package eu.europeana.cloud.service.uis.exception;

import javax.ws.rs.ext.ExceptionMapper;

/**
 * Maps all unhandled runtime exceptions.
 *
 */
public class RuntimeExceptionMapper extends UISExceptionMapper implements
        ExceptionMapper<RuntimeException> {
}
