package eu.europeana.cloud.service.dps.storm.textstripping;

import eu.europeana.cloud.service.dps.DpsTask;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 *
 * @author la4227 <lucas.anastasiou@open.ac.uk>
 */
public class KafkaProducer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9093,localhost:9094");
        props.put("serializer.class", "eu.europeana.cloud.service.dps.storm.JsonEncoder");
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);
        Producer<String, DpsTask> producer = new Producer<String, DpsTask>(config);

        String key = "dataset_for_extraction";
        DpsTask msg = DpsTaskBuilder.generateDpsTask();

        KeyedMessage<String, DpsTask> data = new KeyedMessage<String, DpsTask>(
                TextStrippingConstants.KAFKA_TOPIC, key, msg);
        producer.send(data);
        producer.close();
    }
}
