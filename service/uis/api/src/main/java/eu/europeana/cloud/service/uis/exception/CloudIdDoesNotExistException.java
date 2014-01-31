package eu.europeana.cloud.service.uis.exception;

import eu.europeana.cloud.common.exceptions.GenericException;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.common.response.ErrorInfo;

/**
 * The unique identifier does not exist exception
 * @author Yorgos.Mamakis@ kb.nl
 *
 */
public class CloudIdDoesNotExistException extends GenericException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -881851449536503512L;
	/**
	 * Creates a new instance of this class.
	 * @param e
	 */
	public CloudIdDoesNotExistException(ErrorInfo e){
		super(e);
	}
	
	/**
	 * Creates a new instance of this class.
	 * @param errorInfo
	 */
	public CloudIdDoesNotExistException(IdentifierErrorInfo errorInfo) {
		super(errorInfo);
	}

	

}
