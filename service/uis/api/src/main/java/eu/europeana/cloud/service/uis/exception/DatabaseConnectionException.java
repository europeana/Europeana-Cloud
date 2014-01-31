package eu.europeana.cloud.service.uis.exception;

import eu.europeana.cloud.common.exceptions.GenericException;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.common.response.ErrorInfo;

/**
 * Database connection exception
 * @author Yorgos.Mamakis@ kb.nl
 *
 */
public class DatabaseConnectionException extends GenericException{

	/**
	 * 
	 */
	private static final long serialVersionUID = 2743985314014225235L;
	/**
	 * Creates a new instance of this class.
	 * @param e
	 */
	public DatabaseConnectionException(ErrorInfo e){
		super(e);
	}
	
	/**
	 * Creates a new instance of this class.
	 * @param errorInfo
	 */
	public DatabaseConnectionException(IdentifierErrorInfo errorInfo) {
		super(errorInfo);
	}

	

}
