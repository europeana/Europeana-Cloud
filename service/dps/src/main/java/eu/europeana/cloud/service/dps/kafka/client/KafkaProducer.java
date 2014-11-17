package eu.europeana.cloud.service.dps.kafka.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

// ASSUMPTION: data are produced with the following schema
// KEY<String>: not used;
// VALUE<String>: list of strings with the following format: <data|key1:attribute1;...;...;keyN:attributeN>
// --> three layers of string separators ("|", ";", and ":")

public class KafkaProducer {

	public static void main(String[] args) {
		
        Properties props = new Properties();
        props.put("metadata.broker.list", args[0]);
        //props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("serializer.class", "eu.europeana.cloud.service.dps.kafka.serializer.JsonEncoder");
        //props.put("partitioner.class", "kafka.producer.SimplePartitioner");
        props.put("request.required.acks", "1");
 
        ProducerConfig config = new ProducerConfig(props);
 
        Producer<String, List<String>> producer = new Producer<String, List<String>>(config);
        String key = "";
        List<String> msg = new ArrayList<String>();
        msg.add("data:RECORD|attribute1:RECORD|attribute2:RECORD|attribute3:RECORD");
        msg.add("");
        KeyedMessage<String, List<String>> data = new KeyedMessage<String, List<String>>(args[1], key, msg);
        producer.send(data);
        producer.close();
    }
}