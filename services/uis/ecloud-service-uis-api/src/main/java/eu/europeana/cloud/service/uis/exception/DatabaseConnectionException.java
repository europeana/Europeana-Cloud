package eu.europeana.cloud.service.uis.exception;

import eu.europeana.cloud.service.uis.status.IdentifierErrorInfo;

/**
 * Database connection exception
 * @author Yorgos.Mamakis@ kb.nl
 *
 */
public class DatabaseConnectionException extends GenericException{

	public DatabaseConnectionException(String message){
		super (message);
	}
	
	public DatabaseConnectionException(IdentifierErrorInfo errorInfo) {
		super(errorInfo);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 2743985314014225235L;

}
