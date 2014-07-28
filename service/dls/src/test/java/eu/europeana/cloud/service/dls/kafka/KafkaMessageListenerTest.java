package eu.europeana.cloud.service.dls.kafka;

import eu.europeana.cloud.service.mcs.messages.AbstractMessage;
import eu.europeana.cloud.service.mcs.messages.InsertRepresentationMessage;
import java.util.Random;
import kafka.serializer.DefaultEncoder;
import org.apache.commons.lang.SerializationUtils;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = { "classpath:/testKafkaMessageListener.xml" })
public class KafkaMessageListenerTest {

    @Autowired
    private MessageDispatcher messageDispatcher;

    @Autowired
    private KafkaMessageListener messageListener;

    public KafkaMessageListenerTest() {
    }

    private static final String TOPIC = "topic";
    private static final int PARTITION = 0;
    private static final int OFFSET = 0;

    private final DefaultEncoder encoder = new DefaultEncoder(null);

    @Before
    public void setUp() {
	Mockito.reset(messageDispatcher);
    }

    @Test
    public void shouldSuccessfullyCallOnMessage() {
	// given
	final ArgumentCaptor<AbstractMessage> argument = ArgumentCaptor
		.forClass(AbstractMessage.class);
	final AbstractMessage message = new InsertRepresentationMessage("test");
	final byte[] messageBytes = prepareCorrectMessageBytes(message);
	// when
	messageListener.onMessage(null, PARTITION, OFFSET, null, messageBytes);
	// then
	verify(messageDispatcher, times(1)).routeMessage(argument.capture());
	Mockito.verifyNoMoreInteractions(messageDispatcher);
	assertEquals(message, argument.getValue());

    }

    @Test
    public void shouldIgnoreNullMessageBytesOnMessage() {
	// given
	byte[] messageBytes = null;
	// when
	messageListener.onMessage(null, PARTITION, OFFSET, null, messageBytes);
	// then
	Mockito.verifyZeroInteractions(messageDispatcher);
    }

    @Test
    public void shouldIgnoreNullMsessageBytesOnMessage() {
	// given
	byte[] messageBytes = prepareCorrectMessageBytes(null);
	doThrow(new NullPointerException()).when(messageDispatcher)
		.routeMessage(null);
	// when
	messageListener.onMessage(null, PARTITION, OFFSET, null, messageBytes);
	// then
	verify(messageDispatcher, times(1)).routeMessage(null);
	Mockito.verifyNoMoreInteractions(messageDispatcher);
    }

    @Test
    public void shouldIgnoreMalformedMessageBytesOnMessage() {
	// given
	final byte[] messageBytes = new byte[20];
	new Random().nextBytes(messageBytes);
	// when
	messageListener.onMessage(null, PARTITION, OFFSET, null, messageBytes);
	// then
	Mockito.verifyZeroInteractions(messageDispatcher);
    }

    private byte[] prepareCorrectMessageBytes(AbstractMessage message) {
	return encoder.toBytes(SerializationUtils.serialize(message));
    }

}
