package eu.europeana.cloud.service.uis.exception;

import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class RecordDatasetEmptyExceptionMapper extends UISExceptionMapper implements
		ExceptionMapper<RecordDatasetEmptyException> {

}
