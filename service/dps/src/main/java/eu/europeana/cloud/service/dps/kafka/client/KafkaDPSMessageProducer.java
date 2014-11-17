package eu.europeana.cloud.service.dps.kafka.client;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import eu.europeana.cloud.service.dps.kafka.message.DPSMessage;

// ASSUMPTION: data are produced with the following schema
// KEY<String>: not used;
// VALUE<String>: list of strings with the following format: <data|key1:attribute1;...;...;keyN:attributeN>
// --> three layers of string separators ("|", ";", and ":")

public class KafkaDPSMessageProducer {

	public static void main(String[] args) {

		Properties props = new Properties();
		props.put("metadata.broker.list", args[0]);
		// props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("serializer.class", "eu.europeana.cloud.service.dps.kafka.serializer.JsonEncoder");
		// props.put("partitioner.class", "kafka.producer.SimplePartitioner");
		props.put("request.required.acks", "1");

		ProducerConfig config = new ProducerConfig(props);

		Producer<String, DPSMessage> producer = new Producer<String, DPSMessage>(config);
		String key = "";
		DPSMessage msg = new DPSMessage();
		msg.writeDataEntry("testURI");
		msg.writeAttributeEntry("testAttributes");
		KeyedMessage<String, DPSMessage> data = new KeyedMessage<String, DPSMessage>(args[1], key, msg);
		producer.send(data);
		producer.close();
	}
}