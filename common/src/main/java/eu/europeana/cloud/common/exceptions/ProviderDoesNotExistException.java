package eu.europeana.cloud.common.exceptions;

import eu.europeana.cloud.common.model.IdentifierErrorInfo;

/**
 * Exception thrown when the provider Id does not exist
 * @author Yorgos.Mamakis@ kb.nl
 *
 */
public class ProviderDoesNotExistException extends GenericException {

	/**
	 * Creates a new instance of this class.
	 * @param message
	 */
	public ProviderDoesNotExistException(String message){
		super(message);
	}
	
	/**
	 * Creates a new instance of this class.
	 * @param errorInfo
	 */
	public ProviderDoesNotExistException(IdentifierErrorInfo errorInfo) {
		super(errorInfo);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 468350985177621312L;

}
