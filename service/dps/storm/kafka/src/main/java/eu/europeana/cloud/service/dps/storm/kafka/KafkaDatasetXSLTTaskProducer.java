package eu.europeana.cloud.service.dps.storm.kafka;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import eu.europeana.cloud.service.dps.DpsTask;

public class KafkaDatasetXSLTTaskProducer {

	public static void main(String[] args) {

		// args[0]: metadata broker list (e.g., ecloud.eanadev.org:9093)
		// args[1]: topic name (e.g., franco_maria_topic)
		// args[2]: file containing record URLs (one per line)
		// args[3]: XSLT URL (all records will be processed by this XSLT)
		// args[4]: output extension (do we need an extension as output of the
		// topology?)

		Properties props = new Properties();
		props.put("metadata.broker.list", args[0]);

		// props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("serializer.class",
				"eu.europeana.cloud.service.dps.storm.JsonEncoder");

		// props.put("partitioner.class", "kafka.producer.SimplePartitioner");
		props.put("request.required.acks", "1");

		ProducerConfig config = new ProducerConfig(props);
		Producer<String, DpsTask> producer = new Producer<String, DpsTask>(
				config);

		String key = "";
		DpsTask msg = new DpsTask();

		String line = "";
		BufferedReader br;
		List<String> records = new ArrayList<String>();

		try {
			br = new BufferedReader(new FileReader(args[2]));
			while ((line = br.readLine()) != null) {
				records.add(line.trim());
			}
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		msg.addDataEntry("FILE_URLS", records);
		msg.addParameter("XSLT_URL", args[3]);
		msg.addParameter("OUTPUT_EXT", args[4]);
		KeyedMessage<String, DpsTask> data = new KeyedMessage<String, DpsTask>(
				args[1], key, msg);
		producer.send(data);
		producer.close();
	}
}
