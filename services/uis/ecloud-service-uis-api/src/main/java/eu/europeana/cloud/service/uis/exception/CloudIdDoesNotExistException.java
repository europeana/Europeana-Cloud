package eu.europeana.cloud.service.uis.exception;

import eu.europeana.cloud.service.uis.status.IdentifierErrorInfo;

/**
 * The unique identifier does not exist exception
 * @author Yorgos.Mamakis@ kb.nl
 *
 */
public class CloudIdDoesNotExistException extends GenericException {

	public CloudIdDoesNotExistException(String message){
		super(message);
	}
	public CloudIdDoesNotExistException(IdentifierErrorInfo errorInfo) {
		super(errorInfo);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -881851449536503512L;

}
