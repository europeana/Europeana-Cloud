package eu.europeana.cloud.service.uis.exception;

import eu.europeana.cloud.common.exceptions.GenericException;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;

/**
 * Database connection exception
 * @author Yorgos.Mamakis@ kb.nl
 *
 */
public class DatabaseConnectionException extends GenericException{

	/**
	 * Creates a new instance of this class.
	 * @param message
	 */
	public DatabaseConnectionException(String message){
		super (message);
	}
	
	/**
	 * Creates a new instance of this class.
	 * @param errorInfo
	 */
	public DatabaseConnectionException(IdentifierErrorInfo errorInfo) {
		super(errorInfo);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 2743985314014225235L;

}
