package eu.europeana.cloud.service.mcs.kafka;

import java.util.Properties;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * ProducerWrapper with {@link CustomPartitioner} as routing partitions
 * algorithm is sending message to Kafka broker.
 */
@Component
public class ProducerWrapper {

    private static final Logger LOGGER = LoggerFactory
	    .getLogger(ProducerWrapper.class);

    private final kafka.javaapi.producer.Producer<String, byte[]> kafkaProducer;

    private final String topic;

    /**
     * Constructs a ProducerWrapper.
     * 
     * @param brokerList
     *            list of Kafka broker formated as "host:port,host2:port"
     * @param topic
     *            topic which messages are published
     */
    public ProducerWrapper(String brokerList, String topic) {
	this.topic = topic;
	final Properties properties = new Properties();
	properties.put("metadata.broker.list", brokerList);
	properties.put("serializer.class", "kafka.serializer.DefaultEncoder");
	properties
		.put("key.serializer.class", "kafka.serializer.StringEncoder");
	properties.put("partitioner.class",
		"eu.europeana.cloud.service.mcs.kafka.CustomPartitioner");
	properties.put("producer.type", "sync");
	// the best durability of the messaging
	properties.put("request.required.acks", "-1");
	kafkaProducer = new kafka.javaapi.producer.Producer<String, byte[]>(
		new ProducerConfig(properties));
    }

    /**
     * Sends message to Kafka broker.
     * 
     * @param partitionRoutingKey
     *            key using to route to specified partition via
     *            {@link CustomPartitioner}
     * @param payload
     *            content of message
     */
    public void send(int partitionRoutingKey, byte[] payload) {
	final KeyedMessage<String, byte[]> message = new KeyedMessage(topic,
		Integer.toString(partitionRoutingKey), payload);
	kafkaProducer.send(message);
    }
}
