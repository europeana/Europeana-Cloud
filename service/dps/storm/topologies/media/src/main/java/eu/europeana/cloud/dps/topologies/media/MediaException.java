package eu.europeana.cloud.dps.topologies.media;

/** General class for exceptions generated in media topology. */
public class MediaException extends Exception {
	
	public MediaException(String message, Throwable cause) {
		super(message, cause);
	}
	
	public MediaException(String message) {
		super(message);
	}
	
	public MediaException(Throwable cause) {
		super(cause);
	}
	
}
