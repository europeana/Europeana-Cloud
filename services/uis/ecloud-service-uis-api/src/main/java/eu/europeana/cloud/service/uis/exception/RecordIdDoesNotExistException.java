package eu.europeana.cloud.service.uis.exception;

import eu.europeana.cloud.service.uis.status.IdentifierErrorInfo;

/**
 * Exception thrown when the record id does not exist
 * @author Yorgos.Mamakis@kb.nl
 *
 */
public class RecordIdDoesNotExistException extends GenericException {

	public RecordIdDoesNotExistException(String message){
		super(message);
	}
	
	public RecordIdDoesNotExistException(IdentifierErrorInfo errorInfo) {
		super(errorInfo);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1788394853781008988L;

}
