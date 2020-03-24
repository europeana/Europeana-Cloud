package eu.europeana.cloud.service.dps.service.kafka;

import eu.europeana.cloud.service.dps.DpsRecord;
import eu.europeana.cloud.service.dps.RecordExecutionSubmitService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Stores/retrieves dps records, record's progress and reports from/to Kafka topics.
 */
public class RecordKafkaSubmitService implements RecordExecutionSubmitService {
	private static final Logger LOGGER = LoggerFactory.getLogger(TaskKafkaSubmitService.class);

	private Producer<String, DpsRecord> producer;

	public RecordKafkaSubmitService(String kafkaBroker) {
		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "eu.europeana.cloud.service.dps.service.kafka.util.DpsRecordSerializer");
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		properties.put(ProducerConfig.ACKS_CONFIG, "1");

		producer = new KafkaProducer<String, DpsRecord>(properties);
	}

	@Override
	public void submitRecord(DpsRecord record, String topology) {
		ProducerRecord<String, DpsRecord> data =
				new ProducerRecord<>(topology, record.getTaskId()+"_"+record.getRecordId(), record);
		producer.send(data);
	}

}