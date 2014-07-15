package eu.europeana.cloud.service.dls.kafka;

/**
 * Dispatcher routing message based on message subtype to appropriate
 * message listener.
 * 
 */
import eu.europeana.cloud.service.mcs.messages.AbstractMessage;

public interface Dispatcher {
    /**
     * Routing message based on message subtype to appropriate message listener.
     * 
     * @param message
     *            to be routed
     */
    public <T extends AbstractMessage> void routeMessage(T message);
}
