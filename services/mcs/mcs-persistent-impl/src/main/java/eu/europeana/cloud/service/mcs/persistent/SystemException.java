package eu.europeana.cloud.service.mcs.persistent;

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
