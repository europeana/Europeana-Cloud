package eu.europeana.cloud.service.uis.exception;

import eu.europeana.cloud.common.exceptions.GenericException;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;

/**
 * Thrown whene there is attempt to create a provider that already exists.
 */
public class ProviderAlreadyExistsException extends GenericException {

    


    public ProviderAlreadyExistsException(IdentifierErrorInfo errorInfo) {
        super(errorInfo);
    }
}
