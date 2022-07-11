package eu.europeana.cloud.service.uis.exception;

import eu.europeana.cloud.common.exceptions.GenericException;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.common.response.ErrorInfo;

/**
 * Exception thrown when the providerId/recordId combo has already been mapped to another unique identifier
 * 
 * @deprecated
 */
@Deprecated(since = "7-SNAPSHOT")
public class IdHasBeenMappedException extends GenericException {
	/**
	 * 
	 */
	private static final long serialVersionUID = 3035751589004362690L;
	/**
	 * Creates a new instance of this class.
	 * @param e error info
	 */
	public IdHasBeenMappedException(ErrorInfo e){
		super(e);
	}
	
	/**
	 * Creates a new instance of this class.
	 * @param errorInfo error info
	 */
	public IdHasBeenMappedException(IdentifierErrorInfo errorInfo) {
		super(errorInfo);
	}

	

}
