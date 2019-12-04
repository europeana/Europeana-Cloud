package eu.europeana.cloud.service.dps.service.kafka;


import eu.europeana.cloud.service.dps.DpsRecord;
import eu.europeana.cloud.service.dps.RecordExecutionSubmitService;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Stores/retrieves dps records, record's progress and reports from/to Kafka topics.
 */
public class RecordKafkaSubmitService implements RecordExecutionSubmitService {
	private static final Logger LOGGER = LoggerFactory.getLogger(TaskKafkaSubmitService.class);

	private Producer<String, DpsRecord> producer;

	private String kafkaGroupId;
	private String zookeeperAddress;

	public RecordKafkaSubmitService(String kafkaBroker, String kafkaGroupId, String zookeeperAddress) {
		this.kafkaGroupId = kafkaGroupId;
		this.zookeeperAddress = zookeeperAddress;

		Properties props = new Properties();
		props.put("metadata.broker.list", kafkaBroker);
		props.put("serializer.class", "eu.europeana.cloud.service.dps.service.kafka.util.JsonEncoder");
		props.put("request.required.acks", "1");

		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<>(config);
	}

	@Override
	public void submitRecord(DpsRecord record, String topology) {
		KeyedMessage<String, DpsRecord> data = new KeyedMessage<>(topology, "", record);
		producer.send(data);
	}
}