package eu.europeana.cloud.service.uis.exception;

import eu.europeana.cloud.service.uis.status.IdentifierErrorInfo;

/**
 * Exception thrown when a record does not exist in the database
 * @author Yorgos.Mamakis@ kb.nl
 *
 */
public class RecordDoesNotExistException extends GenericException{

	public RecordDoesNotExistException(String message){
		super(message);
	}
	
	public RecordDoesNotExistException(IdentifierErrorInfo errorInfo) {
		super(errorInfo);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -4115451763281287610L;

}
