package eu.europeana.cloud.service.uis.exception;

import eu.europeana.cloud.service.uis.status.IdentifierErrorInfo;

/**
 * This exception is thrown when the dataset is empty
 * @author Yorgos.Mamakis@ kb.nl
 *
 */
public class RecordDatasetEmptyException extends GenericException {

	public RecordDatasetEmptyException(String message){
		super(message);
	}
	
	public RecordDatasetEmptyException(IdentifierErrorInfo errorInfo) {
		super(errorInfo);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 7827869940617107588L;

}
