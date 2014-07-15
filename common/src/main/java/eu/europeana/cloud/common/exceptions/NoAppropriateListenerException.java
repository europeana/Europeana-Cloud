package eu.europeana.cloud.common.exceptions;

/**
 * Exception thrown when there is no appropriate{@link MessageListener} to
 * message.
 * 
 */
public class NoAppropriateListenerException extends RuntimeException {
    /**
     * {@inheritDoc}
     */
    public NoAppropriateListenerException(String message) {
	super(message);
    }

}
