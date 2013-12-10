package eu.europeana.cloud.service.uis.exception;

import eu.europeana.cloud.service.uis.status.IdentifierErrorInfo;

/**
 * Exception thrown when a record already exists in the database 
 * @author Yorgos.Mamakis@ kb.nl
 *
 */
public class RecordExistsException extends GenericException {

	public RecordExistsException(String message){
		super(message);
	}
	
	public RecordExistsException(IdentifierErrorInfo errorInfo) {
		super(errorInfo);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 3302765474657090505L;

}
