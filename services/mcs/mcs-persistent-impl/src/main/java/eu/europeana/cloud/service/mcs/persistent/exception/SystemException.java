package eu.europeana.cloud.service.mcs.persistent.exception;

/**
 *
 * @author sielski
 */
public class SystemException extends RuntimeException {

	public SystemException() {
	}


	public SystemException(Throwable cause) {
		super(cause);
	}


	public SystemException(String message) {
		super(message);
	}
}
