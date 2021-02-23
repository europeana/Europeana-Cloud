package eu.europeana.cloud.service.dps.storm.topologies.oaipmh;

import eu.europeana.cloud.service.dps.DpsRecord;
import eu.europeana.cloud.service.dps.service.kafka.util.DpsRecordSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SmartProducer {

    //private final Object kafkaBroker="127.0.0.1:9092";
    private final Object kafkaBroker="mandevilla-dev.man.poznan.pl:9092,dipladenia-dev.man.poznan.pl:9092";

    private Producer<String, DpsRecord> kafkaProducer;

    public SmartProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DpsRecordSerializer.class.getName());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "1");

        kafkaProducer = new KafkaProducer<>(properties);
    }

    public void submitRecord(DpsRecord record, String topic) {
        ProducerRecord<String, DpsRecord> data =
                new ProducerRecord<>(topic, record.getTaskId()+"_"+record.getRecordId(), record);
        kafkaProducer.send(data);
    }


    public static void main(String[] args) throws InterruptedException {
        SmartProducer producer = new SmartProducer();
        producer.produce(2,50000);
        producer.close();

    }

    private void close() {
        kafkaProducer.close();
    }

    private void produce(int topicNumber, int messagesCount) {
        for(int i=0;i<messagesCount;i++){
            DpsRecord record = DpsRecord.builder().recordId("recordId"+i).taskId(7555463456887490427L).metadataPrefix(""+topicNumber).build();
            submitRecord(record,"fair_"+topicNumber);
        }

    }

}

