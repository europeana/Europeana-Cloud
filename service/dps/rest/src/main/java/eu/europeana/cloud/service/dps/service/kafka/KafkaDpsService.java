package eu.europeana.cloud.service.dps.service.kafka;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import eu.europeana.cloud.service.dps.DpsService;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.util.DpsTaskUtil;

/**
 * Stores / retrieves dps tasks from / to Kafka topics.
 */
public class KafkaDpsService implements DpsService {
	
	private Producer<String, DpsTask> producer;
	
	private String kafkaTopic;
	private String kafkaBroker;
	
	public KafkaDpsService(String kafkaTopic, String kafkaBroker) {
		
		this.kafkaBroker = kafkaBroker;
		this.kafkaTopic = kafkaTopic;
		
		Properties props = new Properties();
		props.put("metadata.broker.list", kafkaBroker);
		// props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("serializer.class", "eu.europeana.cloud.service.dps.service.kafka.JsonEncoder");
		// props.put("partitioner.class", "kafka.producer.SimplePartitioner");
		props.put("request.required.acks", "1");
		
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, DpsTask>(config);
	}

	@Override
	public void submitTask(DpsTask task) {
		
		String key = "";
		KeyedMessage<String, DpsTask> data = new KeyedMessage<String, DpsTask>(kafkaTopic, key, task);
		producer.send(data);
		producer.close();
	}

	@Override
	public DpsTask fetchAndRemove() {
		
		// TODO does not really fetch it from Kafka
    	DpsTask task = DpsTaskUtil.generateDpsTask();
		return task;
	}
}