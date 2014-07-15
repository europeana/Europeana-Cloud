package eu.europeana.cloud.service.dls.listeners;

import eu.europeana.cloud.service.mcs.messages.AbstractMessage;

/**
 * A MessageListener object is used to receive asynchronously delivered
 * messages.
 * 
 * @param <T>
 *            extends {@link AbstractMessage}
 */
public interface MessageListener<T extends AbstractMessage> {
    /**
     * Passes a message to the listener.
     * 
     * @param message
     */
    public void onMessage(T message);
}
