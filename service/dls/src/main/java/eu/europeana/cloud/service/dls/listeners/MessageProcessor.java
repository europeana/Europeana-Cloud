package eu.europeana.cloud.service.dls.listeners;

import eu.europeana.cloud.service.mcs.messages.AbstractMessage;

/**
 * A MessageProcessor object is used to processing messages.
 * 
 * @param <T>
 *            extends {@link AbstractMessage}
 */
public interface MessageProcessor<T extends AbstractMessage> {
    /**
     * Process message.
     * 
     * @param message
     */
    public void processMessage(T message);
}
