package eu.europeana.cloud.common.model;

import eu.europeana.cloud.common.response.ErrorInfo;

import javax.ws.rs.core.Response.Status;

/**
 * ErrorInfo wrapper with HTTP code information
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * @since Dec 17, 2013
 */
public class IdentifierErrorInfo {

	private Status httpCode;
	
	private ErrorInfo errorInfo;

	public IdentifierErrorInfo(){}
	/**
	 * Creates a new instance of this class.
	 * @param httpCode The http status code
	 * @param errorInfo The error information
	 */
	public IdentifierErrorInfo(Status httpCode, ErrorInfo errorInfo){
		this.httpCode = httpCode;
		this.errorInfo = errorInfo;
				
	}

	/**
	 * @return The HTTP status code
	 */
	public Status getHttpCode() {
		return httpCode;
	}
	
	/**
	 * 
	 * @return The error message
	 */
	public ErrorInfo getErrorInfo() {
		return errorInfo;
	}
	
	
}
