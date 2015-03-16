package eu.europeana.cloud.service.dps.service.kafka;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.message.MessageAndMetadata;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.codehaus.jackson.map.ObjectMapper;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.TaskExecutionSubmitService;

/**
 * Stores / retrieves dps tasks and task progress notifications from / to Kafka
 * topics.
 */
public class KafkaSubmitService implements TaskExecutionSubmitService {

	private Producer<String, DpsTask> producer;
	private ConsumerConnector consumer;

	private String kafkaGroupId;
	private String zookeeperAddress;

	private final static String CONSUMER_TIMEOUT = "1000";
	private final static String ZOOKEEPER_SYNC_TIME = "200";
	private final static String ZOOKEEPER_SESSION_TIMEOUT = "400";
	private final static String AUTOCOMMIT_INTERVAL = "200";

	public KafkaSubmitService(String kafkaBroker, String kafkaGroupId,
			String zookeeperAddress) {

		this.kafkaGroupId = kafkaGroupId;
		this.zookeeperAddress = zookeeperAddress;

		Properties props = new Properties();
		props.put("metadata.broker.list", kafkaBroker);
		props.put("serializer.class", "eu.europeana.cloud.service.dps.service.kafka.util.JsonEncoder");
		props.put("request.required.acks", "1");

		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, DpsTask>(config);
	}

	@Override
	public void submitTask(DpsTask task, String topology) {

		String key = "";
		KeyedMessage<String, DpsTask> data = new KeyedMessage<String, DpsTask>(
				topology, key, task);
		producer.send(data);
	}

	@Override
	public DpsTask fetchTask(String topology) {

		return fetchTaskFromKafka(topology);
	}

	public MessageAndMetadata<byte[], byte[]> fetchKafkaMessage(final String topic) {
		
		consumer = kafka.consumer.Consumer
				.createJavaConsumerConnector(createConsumerConfig(
						zookeeperAddress, kafkaGroupId));

		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, new Integer(1));

		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
				.createMessageStreams(topicCountMap);

		KafkaStream stream = consumerMap.get(topic).get(0);
		ConsumerIterator it = stream.iterator();

		MessageAndMetadata<byte[], byte[]> m = null;
		try {
			
			if (it.hasNext()) {
				m = it.next();
			}
		}
		catch (ConsumerTimeoutException ignore) {
		} 
		finally {
			consumer.commitOffsets();
			consumer.shutdown();
		}

		return m;
	}

	private ConsumerConfig createConsumerConfig(String zookeeperAddress,
			String groupid) {

		Properties props = new Properties();
		props.put("zookeeper.connect", zookeeperAddress);
		props.put("group.id", groupid);
		props.put("zk.sessiontimeout.ms", ZOOKEEPER_SESSION_TIMEOUT);
		props.put("zk.synctime.ms", ZOOKEEPER_SYNC_TIME);
		props.put("autocommit.interval.ms", AUTOCOMMIT_INTERVAL);
		props.put("consumer.timeout.ms", CONSUMER_TIMEOUT);
		return new ConsumerConfig(props);
	}

	private DpsTask fetchTaskFromKafka(String topology) {

		MessageAndMetadata<byte[], byte[]> m = fetchKafkaMessage(topology);
		
		ObjectMapper mapper = new ObjectMapper();
		DpsTask task = null;
		try {
			task = mapper.readValue(m.message(), DpsTask.class);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return task;
	}	
}