package eu.europeana.cloud.service.dls.kafka;

import eu.europeana.cloud.service.mcs.messages.AbstractMessage;

/**
 * Dispatcher that routes message to an appropriate message listener based on message class.
 * 
 */
public interface Dispatcher {
    /**
     * Routes message to an appropriate message listener based on message class.
     * 
     * @param message
     *            to be routed
     */
    <T extends AbstractMessage> void routeMessage(T message);
}
