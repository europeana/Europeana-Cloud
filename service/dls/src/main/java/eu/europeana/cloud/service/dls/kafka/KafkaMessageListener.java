package eu.europeana.cloud.service.dls.kafka;

import com.github.ddth.kafka.IKafkaMessageListener;
import eu.europeana.cloud.service.mcs.messages.AbstractMessage;
import kafka.serializer.DefaultDecoder;
import org.apache.commons.lang.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 
 * Kafka listener receive message from broker and pass them to
 * {@link messageDispatcher#routeMessage}.
 */
@Component
public class KafkaMessageListener implements IKafkaMessageListener {

    private static final Logger LOGGER = LoggerFactory
	    .getLogger(KafkaMessageListener.class);

    @Autowired
    private MessageDispatcher messageDispatcher;

    final DefaultDecoder decoder = new DefaultDecoder(null);

    /**
     * {@inheritDoc}
     */
    @Override
    public void onMessage(String topic, int partition, long offset, byte[] key,
	    byte[] messageBytes) {
	try {
	    AbstractMessage message = (AbstractMessage) SerializationUtils
		    .deserialize(decoder.fromBytes(messageBytes));
	    messageDispatcher.routeMessage(message);
	} catch (Exception e) {
	    // when message is malformed
	    LOGGER.error("Message rejected! Cause:" + e + "\n" + e.getMessage()
		    + "\nRejected message body:" + new String(messageBytes));
	}
    }
}
