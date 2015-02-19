package eu.europeana.cloud.service.dps.service.kafka;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import eu.europeana.cloud.service.dps.DpsService;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.examples.util.DpsTaskUtil;

/**
 * Stores / retrieves dps tasks and task progress notifications
 * 	from / to Kafka topics.
 */
public class KafkaDpsService implements DpsService {

	private Producer<String, DpsTask> producer;
	private final ConsumerConnector consumer;

	private String kafkaBroker;
	private String zookeeperAddress;
	
	private String submitTaskTopic;
	private String genericTaskNotificationTopic;
	private String taskProgressNotificationTopic;
	
	public KafkaDpsService(
			String kafkaBroker,
				String submitTaskTopic, String genericTaskNotificationTopic, String taskProgressNotificationTopic,
					String kafkaGroupId, String zookeeperAddress) {

		this.kafkaBroker = kafkaBroker;
		this.zookeeperAddress = zookeeperAddress;
		
		this.submitTaskTopic = submitTaskTopic;
		this.genericTaskNotificationTopic = genericTaskNotificationTopic;
		this.taskProgressNotificationTopic = taskProgressNotificationTopic;

		this.consumer = kafka.consumer.Consumer
				.createJavaConsumerConnector(createConsumerConfig(zookeeperAddress,
						kafkaGroupId));

		Properties props = new Properties();
		props.put("metadata.broker.list", kafkaBroker);
		props.put("serializer.class", "eu.europeana.cloud.service.dps.storm.JsonEncoder");
		props.put("request.required.acks", "1");

		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, DpsTask>(config);
	}

	@Override
	public void submitTask(DpsTask task) {

		String key = "";
		KeyedMessage<String, DpsTask> data = new KeyedMessage<String, DpsTask>(
				submitTaskTopic, key, task);
		producer.send(data);
		producer.close();
	}

	@Override
	public DpsTask fetchAndRemove() {

		// TODO does not really fetch it from Kafka
		DpsTask task = DpsTaskUtil.generateDpsTask();
		return task;
	}

	@Override
	public String getTaskProgress(String taskId) {
		
		// TODO
		return "50%";
	}

	@Override
	public String getTaskNotification(String taskId) {
		
		// TODO
		return "AllOkForNow";
	}

	public String fetchGenericMessage() {

		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(genericTaskNotificationTopic, new Integer(1));
		
	    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);

		KafkaStream stream = consumerMap.get(genericTaskNotificationTopic).get(0);
		ConsumerIterator it = stream.iterator();

		String m = null;
		while (it.hasNext()) {
			
			m = getMessage(it.next());
			System.out.println(m);
		}
		return m;
	}

	private ConsumerConfig createConsumerConfig(String zookeeperAddress, String groupid) {
		
		Properties props = new Properties();
		props.put("zookeeper.connect", zookeeperAddress);
		props.put("group.id", groupid);
		props.put("zk.sessiontimeout.ms", "400");
		props.put("zk.synctime.ms", "200");
		props.put("autocommit.interval.ms", "1000");
		return new ConsumerConfig(props);
	}

	public static String getMessage(MessageAndMetadata<byte[], byte[]> m) {

//		ByteBuffer buffer = object.payload();
//		byte[] bytes = new byte[buffer.remaining()];
//		buffer.get(bytes);
		return new String(m.message());
	}
}