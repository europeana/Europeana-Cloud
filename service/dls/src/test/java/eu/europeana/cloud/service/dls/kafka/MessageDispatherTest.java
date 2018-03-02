package eu.europeana.cloud.service.dls.kafka;

import eu.europeana.cloud.common.exceptions.NoAppropriateMessageProcessorException;
import eu.europeana.cloud.service.dls.listeners.MessageProcessor;
import eu.europeana.cloud.service.dls.listeners.RepresentationRemovedMessageProcessor;
import eu.europeana.cloud.service.dls.listeners.RepresentationVersionAddedMessageProcessor;
import eu.europeana.cloud.service.mcs.messages.AbstractMessage;
import eu.europeana.cloud.service.mcs.messages.InsertRepresentationMessage;
import eu.europeana.cloud.service.mcs.messages.RemoveRepresentationMessage;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

public class MessageDispatherTest {

    private MessageDispatcher messageDispatcher;

    @Before
    public void setUp() {
	messageDispatcher = new MessageDispatcher();
    }

    @Test
    public void shouldRouteMessage() {
	// given
	String messagePayload = "test";
	RemoveRepresentationMessage message = new RemoveRepresentationMessage(
		messagePayload);
	MessageProcessor<RemoveRepresentationMessage> messageListener = mock(RepresentationRemovedMessageProcessor.class);
	mockDispatcherClass(message, messageListener);
	// when
	messageDispatcher.routeMessage(message);
	// then
	verify(messageListener, times(1)).processMessage(message);
	verifyNoMoreInteractions(messageListener);
    }

    @Test
    public void shouldThrowNoAppropriateListenerExceptionWhenNoListenerDefined() {
	// given
	String messagePayload = "test";
	RemoveRepresentationMessage message = new RemoveRepresentationMessage(
		messagePayload);
	// no route defined in messageDispatcher

	// when
	try {
	    messageDispatcher.routeMessage(message);
	    fail("Not thrown expected exception!");
	} catch (Exception e) {
	    // expected exception
	    assertTrue(e instanceof NoAppropriateMessageProcessorException);
	}
    }

    @Test
    public void shouldThrowsNullPointerExceptionWhenNoListenerDefined() {
	// given
	RemoveRepresentationMessage message = null;
	// no route defined in messageDispatcher

	// when
	try {
	    messageDispatcher.routeMessage(message);
	    fail("Not thrown expected exception!");
	} catch (Exception e) {
	    // expected exception
	    assertTrue(e instanceof NullPointerException);
	}
    }

    @Test
    public void shouldThrowNoAppropriateListenerExceptionWhenNoAppropriateListenerDefined() {
	// given
	String messagePayload = "test";
	RemoveRepresentationMessage message = new RemoveRepresentationMessage(
		messagePayload);
	InsertRepresentationMessage iRMessage = new InsertRepresentationMessage(
		messagePayload);
	MessageProcessor<InsertRepresentationMessage> iRMessageListener = mock(RepresentationVersionAddedMessageProcessor.class);
	mockDispatcherClass(iRMessage, iRMessageListener);

	// no route to RemoveRepresentationMessage defined in messageDispatcher
	try {
	    // when
	    messageDispatcher.routeMessage(message);
	    fail("No expected exception!");
	} catch (Exception e) {
	    // expected exception
	    assertTrue(e instanceof NoAppropriateMessageProcessorException);
	    verifyZeroInteractions(iRMessageListener);
	}
    }

    @Test
    public void shouldRouteMessageWhenTwoListeners() {
        // given
        String messagePayload = "test";
        Map<Class<? extends AbstractMessage>, MessageProcessor<? extends AbstractMessage>> listenersMap
                = new HashMap<>();

        // InsertRepresentationMessage add to map
        InsertRepresentationMessage iRMessage = new InsertRepresentationMessage(
                messagePayload);
        MessageProcessor<InsertRepresentationMessage> iRMessageListener = mock(RepresentationVersionAddedMessageProcessor.class);
        listenersMap.put(iRMessage.getClass(), iRMessageListener);
        // RemoveRepresentationMessage add to map   
        RemoveRepresentationMessage rRMessage = new RemoveRepresentationMessage(
                messagePayload);
        MessageProcessor<RemoveRepresentationMessage> rRMessageListener = mock(RepresentationRemovedMessageProcessor.class);
        listenersMap.put(rRMessage.getClass(), rRMessageListener);
        messageDispatcher.setListenersMap(listenersMap);

        // when
        messageDispatcher.routeMessage(iRMessage);
        // then
        verify(iRMessageListener, times(1)).processMessage(iRMessage);
        verifyNoMoreInteractions(iRMessageListener);
        verifyZeroInteractions(rRMessageListener);
    }

    private <T extends AbstractMessage> void mockDispatcherClass(T messageClass, MessageProcessor<T> messageListener) {
        Map<Class<? extends AbstractMessage>, MessageProcessor<? extends AbstractMessage>> listenersMap
                = new HashMap<>();
        listenersMap.put(messageClass.getClass(), messageListener);
        messageDispatcher.setListenersMap(listenersMap);
    }
}
