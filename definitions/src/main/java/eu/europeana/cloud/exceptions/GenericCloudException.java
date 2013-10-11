package eu.europeana.cloud.exceptions;

import eu.europeana.cloud.definitions.StatusCode;

public class GenericCloudException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8278732402757412469L;

	public String message;
	public StatusCode errorCode;

	public GenericCloudException(String message, StatusCode errorCode) {
		super();
		this.errorCode = errorCode;
		this.message = message;
	}

	@Override
	public String getMessage() {
		return this.message;
	}

	public StatusCode getErrorCode() {
		return this.errorCode;
	}
}
