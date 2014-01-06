package eu.europeana.cloud.service.uis.exception;

import eu.europeana.cloud.common.exceptions.GenericException;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.common.response.ErrorInfo;

/**
 * Exception thrown when a record does not exist in the database
 * @author Yorgos.Mamakis@ kb.nl
 *
 */
public class RecordDoesNotExistException extends GenericException{
	
	/**
	 * Creates a new instance of this class.
	 * @param errorInfo
	 */
	public RecordDoesNotExistException(ErrorInfo errorInfo) {
		super(errorInfo);
	}
	
	
	/**
	 * Creates a new instance of this class.
	 * @param errorInfo
	 */
	public RecordDoesNotExistException(IdentifierErrorInfo errorInfo) {
		super(errorInfo);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -4115451763281287610L;

}
