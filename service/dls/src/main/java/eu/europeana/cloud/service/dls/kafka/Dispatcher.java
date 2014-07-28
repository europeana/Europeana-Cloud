package eu.europeana.cloud.service.dls.kafka;

/**
 * Dispatcher routes message to an appropriate message listener based on message class.
 * 
 */
import eu.europeana.cloud.service.mcs.messages.AbstractMessage;

public interface Dispatcher {
    /**
     * Routes message to an appropriate message listener based on message class.
     * 
     * @param message
     *            to be routed
     */
    public <T extends AbstractMessage> void routeMessage(T message);
}
