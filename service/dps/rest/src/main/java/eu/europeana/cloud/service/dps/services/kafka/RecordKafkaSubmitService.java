package eu.europeana.cloud.service.dps.services.kafka;

import eu.europeana.cloud.service.dps.DpsRecord;
import eu.europeana.cloud.service.dps.DpsRecordSerializer;
import eu.europeana.cloud.service.dps.RecordExecutionSubmitService;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Stores/retrieves dps records, record's progress and reports from/to Kafka topics.
 */
public class RecordKafkaSubmitService implements RecordExecutionSubmitService {
    private static final Logger LOGGER = LoggerFactory.getLogger(RecordKafkaSubmitService.class);
    private final TaskStatusUpdater taskStatusUpdater;
    private final Producer<String, DpsRecord> producer;

    public RecordKafkaSubmitService(String kafkaBroker, TaskStatusUpdater taskStatusUpdater) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DpsRecordSerializer.class.getName());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        producer = new KafkaProducer<>(properties);
        this.taskStatusUpdater = taskStatusUpdater;
    }

    @Override
    public void submitRecord(DpsRecord dpsRecord, String topic) {
        ProducerRecord<String, DpsRecord> data =
                new ProducerRecord<>(topic, dpsRecord.getTaskId() + "_" + dpsRecord.getRecordId(), dpsRecord);
        producer.send(data,
                (metadata, exception) -> {
                    if (exception != null) {
                        LOGGER.error("Dropping the task {} because of", dpsRecord.getTaskId(), exception);
                        taskStatusUpdater.setTaskDropped(dpsRecord.getTaskId(), ExceptionUtils.getStackTrace(exception));
                    }
                });
    }
}