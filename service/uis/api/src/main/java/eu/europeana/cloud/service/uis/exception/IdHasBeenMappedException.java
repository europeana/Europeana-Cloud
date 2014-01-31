package eu.europeana.cloud.service.uis.exception;

import eu.europeana.cloud.common.exceptions.GenericException;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.common.response.ErrorInfo;

/**
 * Exception thrown when the providerId/recordId combo has already been mapped
 * to another unique identifier
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * 
 */
public class IdHasBeenMappedException extends GenericException {

	
	/**
	 * 
	 */
	private static final long serialVersionUID = 3035751589004362690L;
	/**
	 * Creates a new instance of this class.
	 * @param e
	 */
	public IdHasBeenMappedException(ErrorInfo e){
		super(e);
	}
	
	/**
	 * Creates a new instance of this class.
	 * @param errorInfo
	 */
	public IdHasBeenMappedException(IdentifierErrorInfo errorInfo) {
		super(errorInfo);
	}

	

}
