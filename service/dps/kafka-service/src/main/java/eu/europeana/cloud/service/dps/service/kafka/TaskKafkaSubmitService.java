package eu.europeana.cloud.service.dps.service.kafka;


import com.google.common.base.Throwables;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.TaskExecutionSubmitService;
/*
import kafka.consumer.Conconsumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.message.MessageAndMetadata;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
*/
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Stores / retrieves dps tasks and task progress and reports from / to Kafka
 * topics.
 */
public class TaskKafkaSubmitService implements TaskExecutionSubmitService {

	private Producer<String, DpsTask> producer;

	//private String kafkaGroupId;
	//private String zookeeperAddress;

	//private final static String CONSUMER_TIMEOUT = "1000";
	//private final static String ZOOKEEPER_SYNC_TIME = "200";
	//private final static String ZOOKEEPER_SESSION_TIMEOUT = "400";
	//private final static String AUTOCOMMIT_INTERVAL = "200";

	//private static final Logger LOGGER = LoggerFactory.getLogger(TaskKafkaSubmitService.class);

	public TaskKafkaSubmitService(String kafkaBroker) {

		//this.kafkaGroupId = kafkaGroupId;
		//this.zookeeperAddress = zookeeperAddress;

		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "eu.europeana.cloud.service.dps.service.kafka.util.DpsTaskSerializer");
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		properties.put(ProducerConfig.ACKS_CONFIG, "1");

		producer = new KafkaProducer<String, DpsTask>(properties);
	}

	@Override
	public void submitTask(DpsTask task, String topology) {
		ProducerRecord<String, DpsTask> data =
				new ProducerRecord<>(topology, String.valueOf(task.getTaskId()), task);
		producer.send(data);
	}

/*	@Override
	public DpsTask fetchTask(String topology, long taskId) {
		return fetchTaskFromKafka(topology);
	}

	public MessageAndMetadata<byte[], byte[]> fetchKafkaMessage(final String topic) {
		
		ConsumerConnector consumer = kafka.consumer.Consumer
				.createJavaConsumerConnector(createConsumerConfig(
						zookeeperAddress, kafkaGroupId));

		Map<String, Integer> topicCountMap = new HashMap<>();
		topicCountMap.put(topic, 1);

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

	private Properties createConsumerConfig(String zookeeperAddress, String groupId) {
		Properties properties = new Properties();
		properties.put("zookeeper.connect", zookeeperAddress);
		properties.put("zk.sessiontimeout.ms", ZOOKEEPER_SESSION_TIMEOUT);
		// zookeeper.session.timeout.ms

		properties.put("zk.synctime.ms", ZOOKEEPER_SYNC_TIME);
		// zookeeper.sync.time.ms

		properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.put("autocommit.interval.ms", AUTOCOMMIT_INTERVAL);
		//auto.commit.interval.ms zależne od enable.auto.commit (default true)

		properties.put("consumer.timeout.ms", CONSUMER_TIMEOUT);

		ConsumerConfig a;

		return properties;
	}

	private DpsTask fetchTaskFromKafka(String topology) {

		MessageAndMetadata<byte[], byte[]> m = fetchKafkaMessage(topology);
		
		ObjectMapper mapper = new ObjectMapper();
		DpsTask task = null;
		try {
			task = mapper.readValue(m.message(), DpsTask.class);
		} catch (IOException e) {
			LOGGER.error(Throwables.getStackTraceAsString(e));
		}
		
		return task;
	}	*/
}