package eu.europeana.cloud.service.uis.exception;

import eu.europeana.cloud.common.exceptions.GenericException;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.common.response.ErrorInfo;

/**
 * Thrown when there is attempt to create a provider that already exists.
 */
public class ProviderAlreadyExistsException extends GenericException {

    
	/**
	 * Creates a new instance of this class.
	 * @param e
	 */
	public ProviderAlreadyExistsException(ErrorInfo e){
		super(e);
	}
	

    /**
     * Creates a new instance of this class.
     * @param errorInfo
     */
    public ProviderAlreadyExistsException(IdentifierErrorInfo errorInfo) {
        super(errorInfo);
    }
}
