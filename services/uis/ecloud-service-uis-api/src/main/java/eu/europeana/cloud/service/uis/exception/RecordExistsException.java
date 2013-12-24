package eu.europeana.cloud.service.uis.exception;

import eu.europeana.cloud.common.exceptions.GenericException;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;

/**
 * Exception thrown when a record already exists in the database 
 * @author Yorgos.Mamakis@ kb.nl
 *
 */
public class RecordExistsException extends GenericException {

	/**
	 * Creates a new instance of this class.
	 * @param message
	 */
	public RecordExistsException(String message){
		super(message);
	}
	
	/**
	 * Creates a new instance of this class.
	 * @param errorInfo
	 */
	public RecordExistsException(IdentifierErrorInfo errorInfo) {
		super(errorInfo);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 3302765474657090505L;

}
