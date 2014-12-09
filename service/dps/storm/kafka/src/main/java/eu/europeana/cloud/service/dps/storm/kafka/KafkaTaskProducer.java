package eu.europeana.cloud.service.dps.storm.kafka;

import java.util.ArrayList;
import java.util.Properties;

import eu.europeana.cloud.service.dps.DpsTask;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

//import eu.europeana.cloud.service.dps.kafka.message.DPSMessage;

public class KafkaTaskProducer {

	public static void main(String[] args) {

		Properties props = new Properties();
		props.put("metadata.broker.list", args[0]);
		// props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("serializer.class",
				"eu.europeana.cloud.service.dps.kafka.serializer.JsonEncoder");
		// props.put("partitioner.class", "kafka.producer.SimplePartitioner");
		props.put("request.required.acks", "1");

		ProducerConfig config = new ProducerConfig(props);

		Producer<String, DpsTask> producer = new Producer<String, DpsTask>(config);
		String key = "";
		DpsTask msg = new DpsTask();
		msg.addDataEntry("URI", new ArrayList<String>());
		msg.addParameter("XSLT_URL", "URL");
		KeyedMessage<String, DpsTask> data = new KeyedMessage<String, DpsTask>(args[1], key, msg);
		producer.send(data);
		producer.close();
	}
}
