package eu.europeana.cloud.common.exceptions;

/**
 * Exception thrown when there is no appropriate{@link MessageProcessor} to
 * message.
 * 
 */
public class NoAppropriateMessageProcessorException extends RuntimeException {
    /**
     * {@inheritDoc}
     */
    public NoAppropriateMessageProcessorException(String message) {
	super(message);
    }

}
