package eu.europeana.cloud.service.dps.service.kafka;


import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.TaskExecutionSubmitService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Stores / retrieves dps tasks and task progress and reports from / to Kafka
 * topics.
 */
public class TaskKafkaSubmitService implements TaskExecutionSubmitService {

	private Producer<String, DpsTask> producer;

	public TaskKafkaSubmitService(String kafkaBroker) {
		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "eu.europeana.cloud.service.dps.service.kafka.util.DpsTaskSerializer");
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		properties.put(ProducerConfig.ACKS_CONFIG, "1");

		producer = new KafkaProducer<>(properties);
	}

	@Override
	public void submitTask(DpsTask task, String topology) {
		ProducerRecord<String, DpsTask> data =
				new ProducerRecord<>(topology, String.valueOf(task.getTaskId()), task);
		producer.send(data);
	}
}