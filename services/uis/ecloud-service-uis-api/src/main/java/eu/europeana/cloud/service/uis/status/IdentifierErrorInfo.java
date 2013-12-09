package eu.europeana.cloud.service.uis.status;

import javax.ws.rs.core.Response.Status;

import eu.europeana.cloud.common.response.ErrorInfo;

public class IdentifierErrorInfo {

	private Status httpCode;
	
	private ErrorInfo errorInfo;
	
	public IdentifierErrorInfo(Status httpCode, ErrorInfo errorInfo){
		this.httpCode = httpCode;
		this.errorInfo = errorInfo;
				
	}

	public Status getHttpCode() {
		return httpCode;
	}

	public ErrorInfo getErrorInfo() {
		return errorInfo;
	}
	
	
}
