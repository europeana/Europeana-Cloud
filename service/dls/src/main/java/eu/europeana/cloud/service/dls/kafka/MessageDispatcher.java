package eu.europeana.cloud.service.dls.kafka;

import eu.europeana.cloud.common.exceptions.NoAppropriateListenerException;
import eu.europeana.cloud.service.dls.kafka.Dispatcher;
import eu.europeana.cloud.service.dls.listeners.MessageListener;
import eu.europeana.cloud.service.mcs.messages.AbstractMessage;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * MessageDispatcher routing message based on message subtype to appropriate
 * message listener.
 * 
 */
@Component
public class MessageDispatcher implements Dispatcher {

    private static final Logger LOGGER = LoggerFactory
	    .getLogger(MessageDispatcher.class);

    private Map<Class<? extends AbstractMessage>, MessageListener<? extends AbstractMessage>> listenersMap = new HashMap<Class<? extends AbstractMessage>, MessageListener<? extends AbstractMessage>>();

    public void setListenersMap(
	    Map<Class<? extends AbstractMessage>, MessageListener<? extends AbstractMessage>> listenersMap) {
	this.listenersMap = listenersMap;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T extends AbstractMessage> void routeMessage(T message) {
	final MessageListener<AbstractMessage> listener = (MessageListener<AbstractMessage>) listenersMap
		.get(message.getClass());
	if (listener == null) {
	    throw new NoAppropriateListenerException(
		    "No route set to specified message type:"
			    + message.getClass());
	}
	listener.onMessage(message);
    }

}
