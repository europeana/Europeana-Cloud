package eu.europeana.cloud.dps.topologies.media.support;

/** General class for exceptions generated in media topology. */
public class MediaException extends Exception {
	
	public final String reportError;
	
	public MediaException(String message, String reportError, Throwable cause) {
		super(message, cause);
		this.reportError = reportError;
	}
	
	public MediaException(String message, String reportError) {
		this(message, reportError, null);
	}
	
	public MediaException(String message, Throwable cause) {
		this(message, null, cause);
	}
	
	public MediaException(String message) {
		this(message, null, null);
	}
}
