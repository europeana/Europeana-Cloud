package eu.europeana.cloud.service.uis.exception;

import eu.europeana.cloud.service.uis.status.IdentifierErrorInfo;

/**
 * Generic Exception
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * @since Dec 17, 2013
 */
public class GenericException extends Exception {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -6146223626718871100L;
	private IdentifierErrorInfo errorInfo;
	
	/**
	 * Creates a new instance of this class.
	 * @param message
	 */
	public GenericException(String message){
		super(message);
	}
	
	/**
	 * Creates a new instance of this class.
	 * @param errorInfo
	 */
	public GenericException (IdentifierErrorInfo errorInfo){
		super();
		this.errorInfo = errorInfo;
	}
	
	/**
	 * Retrieve the error information that caused the exception happen
	 * @return The error information
	 */
	public IdentifierErrorInfo getErrorInfo() {
		return this.errorInfo;
	}
}
