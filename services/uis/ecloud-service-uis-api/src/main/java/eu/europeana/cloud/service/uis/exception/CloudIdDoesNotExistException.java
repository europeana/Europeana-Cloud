package eu.europeana.cloud.service.uis.exception;

import eu.europeana.cloud.common.exceptions.GenericException;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;

/**
 * The unique identifier does not exist exception
 * @author Yorgos.Mamakis@ kb.nl
 *
 */
public class CloudIdDoesNotExistException extends GenericException {

	/**
	 * Creates a new instance of this class.
	 * @param message
	 */
	public CloudIdDoesNotExistException(String message){
		super(message);
	}
	/**
	 * Creates a new instance of this class.
	 * @param errorInfo
	 */
	public CloudIdDoesNotExistException(IdentifierErrorInfo errorInfo) {
		super(errorInfo);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -881851449536503512L;

}
