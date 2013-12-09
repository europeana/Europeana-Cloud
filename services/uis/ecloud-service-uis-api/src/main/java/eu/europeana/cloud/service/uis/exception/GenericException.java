package eu.europeana.cloud.service.uis.exception;

import eu.europeana.cloud.service.uis.status.IdentifierErrorInfo;

public class GenericException extends Exception {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -6146223626718871100L;
	private IdentifierErrorInfo errorInfo;
	
	public GenericException (IdentifierErrorInfo errorInfo){
		super();
		this.errorInfo = errorInfo;
	}
	
	public IdentifierErrorInfo getErrorInfo() {
		return errorInfo;
	}
}
