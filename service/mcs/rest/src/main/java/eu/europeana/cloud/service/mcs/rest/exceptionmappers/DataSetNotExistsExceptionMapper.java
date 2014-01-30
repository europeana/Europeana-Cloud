package eu.europeana.cloud.service.mcs.rest.exceptionmappers;

import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;

/**
 * Maps {@link DataSetNotExistsExceptionMapper} to {@link javax.ws.rs.core.Response}.
 * 
 * @author marcinm@man.poznan.pl
 * @see javax.ws.rs.ext.ExceptionMapper
 */
@Provider
public class DataSetNotExistsExceptionMapper extends UnitedExceptionMapper implements
        ExceptionMapper<DataSetNotExistsException> {
}
