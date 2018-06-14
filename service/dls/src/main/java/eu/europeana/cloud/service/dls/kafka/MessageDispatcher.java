package eu.europeana.cloud.service.dls.kafka;

import eu.europeana.cloud.common.exceptions.NoAppropriateMessageProcessorException;
import eu.europeana.cloud.service.dls.listeners.MessageProcessor;
import eu.europeana.cloud.service.mcs.messages.AbstractMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * MessageDispatcher routes message to an appropriate {@link MessageProcessor} based on message class.
 * Initializes  listenersMap - a routing table containg message class - {@link MessageProcessor} class associations. 
 */
@Component
public class MessageDispatcher implements Dispatcher {

    private static final Logger LOGGER = LoggerFactory
	    .getLogger(MessageDispatcher.class);

    private Map<Class<? extends AbstractMessage>, MessageProcessor<? extends AbstractMessage>> listenersMap = new HashMap<>();

    public void setListenersMap(
	    Map<Class<? extends AbstractMessage>, MessageProcessor<? extends AbstractMessage>> listenersMap) {
	this.listenersMap = listenersMap;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T extends AbstractMessage> void routeMessage(T message) {
	final MessageProcessor<AbstractMessage> listener = (MessageProcessor<AbstractMessage>) listenersMap
		.get(message.getClass());
	if (listener == null) {
	    throw new NoAppropriateMessageProcessorException(
		    "No route set to specified message type:"
			    + message.getClass());
	}
	listener.processMessage(message);
    }

}
