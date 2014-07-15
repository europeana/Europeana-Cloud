package eu.europeana.cloud.service.dls.kafka;

import eu.europeana.cloud.common.exceptions.NoAppropriateListenerException;
import eu.europeana.cloud.service.dls.listeners.MessageListener;
import eu.europeana.cloud.service.dls.listeners.RepresentationRemovedListener;
import eu.europeana.cloud.service.dls.listeners.RepresentationVersionAddedListener;
import eu.europeana.cloud.service.mcs.messages.AbstractMessage;
import eu.europeana.cloud.service.mcs.messages.InsertRepresentationMessage;
import eu.europeana.cloud.service.mcs.messages.RemoveRepresentationMessage;
import java.util.HashMap;
import java.util.Map;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;

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
	MessageListener<RemoveRepresentationMessage> messageListener = mock(RepresentationRemovedListener.class);
	mockDispatcherClass(message, messageListener);
	// when
	messageDispatcher.routeMessage(message);
	// then
	verify(messageListener, times(1)).onMessage(message);
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
	    fail("No expected exception!");
	} catch (Exception e) {
	    // expected exception
	    assertTrue(e instanceof NoAppropriateListenerException);
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
	MessageListener<InsertRepresentationMessage> iRMessageListener = mock(RepresentationVersionAddedListener.class);
	mockDispatcherClass(iRMessage, iRMessageListener);

	// no route to RemoveRepresentationMessage defined in messageDispatcher
	try {
	    // when
	    messageDispatcher.routeMessage(message);
	    fail("No expected exception!");
	} catch (Exception e) {
	    // expected exception
	    assertTrue(e instanceof NoAppropriateListenerException);
	    verifyZeroInteractions(iRMessageListener);
	}
    }

    @Test
    public void shouldRouteMessageWhenTwoListeners() {
        // given
        String messagePayload = "test";
        Map<Class<? extends AbstractMessage>, MessageListener<? extends AbstractMessage>> listenersMap
                = new HashMap<>();

        // InsertRepresentationMessage add to map
        InsertRepresentationMessage iRMessage = new InsertRepresentationMessage(
                messagePayload);
        MessageListener<InsertRepresentationMessage> iRMessageListener = mock(RepresentationVersionAddedListener.class);
        listenersMap.put(iRMessage.getClass(), iRMessageListener);
        // RemoveRepresentationMessage add to map   
        RemoveRepresentationMessage rRMessage = new RemoveRepresentationMessage(
                messagePayload);
        MessageListener<RemoveRepresentationMessage> rRMessageListener = mock(RepresentationRemovedListener.class);
        listenersMap.put(rRMessage.getClass(), rRMessageListener);
        messageDispatcher.setListenersMap(listenersMap);

        // when
        messageDispatcher.routeMessage(iRMessage);
        // then
        verify(iRMessageListener, times(1)).onMessage(iRMessage);
        verifyNoMoreInteractions(iRMessageListener);
        verifyZeroInteractions(rRMessageListener);
    }

    private <T extends AbstractMessage> void mockDispatcherClass(T messageClass, MessageListener<T> messageListener) {
        Map<Class<? extends AbstractMessage>, MessageListener<? extends AbstractMessage>> listenersMap
                = new HashMap<>();
        listenersMap.put(messageClass.getClass(), messageListener);
        messageDispatcher.setListenersMap(listenersMap);
    }
}
