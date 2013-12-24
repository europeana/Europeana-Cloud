package eu.europeana.cloud.service.uis.exception;

import eu.europeana.cloud.common.exceptions.GenericException;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;

/**
 * This exception is thrown when the dataset is empty
 * @author Yorgos.Mamakis@ kb.nl
 *
 */
public class RecordDatasetEmptyException extends GenericException {

	/**
	 * Creates a new instance of this class.
	 * @param message
	 */
	public RecordDatasetEmptyException(String message){
		super(message);
	}
	
	/**
	 * Creates a new instance of this class.
	 * @param errorInfo
	 */
	public RecordDatasetEmptyException(IdentifierErrorInfo errorInfo) {
		super(errorInfo);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 7827869940617107588L;

}
