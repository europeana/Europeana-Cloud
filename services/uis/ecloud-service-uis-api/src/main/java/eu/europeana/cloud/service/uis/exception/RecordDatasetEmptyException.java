package eu.europeana.cloud.service.uis.exception;

import eu.europeana.cloud.common.exceptions.GenericException;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.common.response.ErrorInfo;

/**
 * This exception is thrown when the dataset is empty
 * @author Yorgos.Mamakis@ kb.nl
 *
 */
public class RecordDatasetEmptyException extends GenericException {

	public RecordDatasetEmptyException(ErrorInfo e){
		super(e);
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
