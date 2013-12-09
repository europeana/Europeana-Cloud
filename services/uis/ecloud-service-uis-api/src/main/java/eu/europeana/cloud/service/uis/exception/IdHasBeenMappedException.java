package eu.europeana.cloud.service.uis.exception;

import eu.europeana.cloud.service.uis.status.IdentifierErrorInfo;

/**
 * Exception thrown when the providerId/recordId combo has already been mapped
 * to another unique identifier
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * 
 */
public class IdHasBeenMappedException extends GenericException {

	public IdHasBeenMappedException(IdentifierErrorInfo errorInfo) {
		super(errorInfo);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 3035751589004362690L;

}
