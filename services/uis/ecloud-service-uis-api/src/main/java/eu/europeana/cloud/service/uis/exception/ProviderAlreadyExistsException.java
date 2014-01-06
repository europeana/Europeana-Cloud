package eu.europeana.cloud.service.uis.exception;

import eu.europeana.cloud.common.exceptions.GenericException;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.common.response.ErrorInfo;

/**
 * Thrown whene there is attempt to create a provider that already exists.
 */
public class ProviderAlreadyExistsException extends GenericException {

    
	public ProviderAlreadyExistsException(ErrorInfo e){
		super(e);
	}
	

    public ProviderAlreadyExistsException(IdentifierErrorInfo errorInfo) {
        super(errorInfo);
    }
}
