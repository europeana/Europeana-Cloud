package eu.europeana.cloud.service.dps.services.kafka;

import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.dps.DpsRecord;
import eu.europeana.cloud.service.dps.DpsRecordSerializer;
import eu.europeana.cloud.service.dps.RecordExecutionSubmitService;
import eu.europeana.cloud.service.dps.exception.KafkaSubmissionException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * Stores/retrieves dps records, record's progress and reports from/to Kafka topics.
 */
public class RecordKafkaSubmitService implements RecordExecutionSubmitService {

  //Together with default ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG 30,000, it gives about one minute between retries.
  public static final int SLEEP_TIME_BETWEEN_RETRIES_MS = 30_000;
  public static final int MAX_ATTEMPTS = 10;
  private final Producer<String, DpsRecord> producer;

  public RecordKafkaSubmitService(String kafkaBroker) {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DpsRecordSerializer.class.getName());
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    producer = new KafkaProducer<>(properties);
  }

  @Override
  public void submitRecord(DpsRecord dpsRecord, String topic) {
    ProducerRecord<String, DpsRecord> data =
        new ProducerRecord<>(topic, dpsRecord.getTaskId() + "_" + dpsRecord.getRecordId(), dpsRecord);
    try {
      RetryableMethodExecutor.execute("Could not send record to Kafka: " + dpsRecord,
          MAX_ATTEMPTS, SLEEP_TIME_BETWEEN_RETRIES_MS, () -> producer.send(data).get());
    } catch (ExecutionException e) {
      throw new KafkaSubmissionException("Could not send record to Kafka: " + dpsRecord, e);
    }
  }
}
