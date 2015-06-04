package eu.europeana.cloud.service.dps.examples.xslt;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import eu.europeana.cloud.service.dps.DpsTask;

public class KafkaXSLTTaskProducer {

	public static void main(String[] args) {

		// args[0]: metadata broker list (e.g., ecloud.eanadev.org:9093)
		// args[1]: topic name (e.g., franco_maria_topic)
		
		// args[2]: record URI 
		//	(e.g 
		// http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.3-SNAPSHOT/records/"
		// + "L9WSPSMVQ85/representations/edm/versions/b17c4f60-70d0-11e4-8fe1-00163eefc9c8/files/af7d3a77-4b00-485f-832c-a33c5a3d7b56
		//	)
		
		// args[3]: XSLT URL (all records will be processed by this XSLT) 
		// http://ecloud.eanadev.org:8080/hera/sample_xslt.xslt
		
		// args[4]: output extension (do we need an extension as output of the
		// topology?)

		Properties props = new Properties();
		props.put("metadata.broker.list", args[0]);

		props.put("serializer.class", "eu.europeana.cloud.service.dps.storm.kafka.JsonEncoder");
		props.put("request.required.acks", "1");

		ProducerConfig config = new ProducerConfig(props);
		Producer<String, DpsTask> producer = new Producer<String, DpsTask>(
				config);

		String key = "";
		DpsTask msg = new DpsTask();

		List<String> records = new ArrayList<String>();
		records.add(args[2]);
		msg.addDataEntry("FILE_URLS", records);
		msg.addParameter("XSLT_URL", args[3]);
		KeyedMessage<String, DpsTask> data = new KeyedMessage<String, DpsTask>(
				args[1], key, msg);
		producer.send(data);
		producer.close();
	}
}
