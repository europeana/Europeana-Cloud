package eu.europeana.cloud.service.dps.storm.topologies.xslt;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import eu.europeana.cloud.service.dps.storm.KafkaMetricsConsumer;
import eu.europeana.cloud.service.dps.storm.ProgressBolt;
import eu.europeana.cloud.service.dps.storm.io.ReadFileBolt;
import eu.europeana.cloud.service.dps.storm.io.WriteRecordBolt;
import eu.europeana.cloud.service.dps.storm.kafka.KafkaParseTaskBolt;
import eu.europeana.cloud.service.dps.storm.xslt.XsltBolt;

public class XSLTTopology {

	// private static String ecloudMcsAddress =
	// "http://146.48.82.158:8080/ecloud-service-mcs-rest-0.3-SNAPSHOT";
	private static String ecloudMcsAddress = "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.3-SNAPSHOT";
	// private static String username = "Cristiano";
	// private static String password = "Ronaldo";

	private static String username = "admin";
	private static String password = "admin";

	// private static String username = "Emmanouil_Koufakis";
	// private static String password = "J9vdq9rpPy";
	private static String zkAddress = "ecloud.eanadev.org:2181";

	private static String kafkaTopic = "storm_metrics_topic";
	private static String kafkaBroker = "ecloud.eanadev.org:9093";

	public static final Logger LOGGER = LoggerFactory
			.getLogger(XSLTTopology.class);

	private final BrokerHosts brokerHosts;

	public XSLTTopology(String kafkaZookeeper) {
		brokerHosts = new ZkHosts(kafkaZookeeper);
	}

	public StormTopology buildTopology() {
		
		int numberOfExecutors = 16;
		int numberOfTasks = 16;
		
		ReadFileBolt retrieveFileBolt = new ReadFileBolt(zkAddress,
				ecloudMcsAddress, username, password);
		WriteRecordBolt writeRecordBolt = new WriteRecordBolt(ecloudMcsAddress,
				username, password);
		ProgressBolt progressBolt = new ProgressBolt(zkAddress);

		SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts,
				"franco_maria_topic_2", "", "storm");
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		TopologyBuilder builder = new TopologyBuilder();

		// TOPOLOGY STRUCTURE!
		// 1 executor, i.e., 1 thread.
		// 1 task per executor
		builder.setSpout("kafkaReader", new KafkaSpout(kafkaConfig), 1)
				.setNumTasks(numberOfTasks);

		builder.setBolt("parseKafkaInput", new KafkaParseTaskBolt(), numberOfExecutors)
				.setNumTasks(numberOfTasks).shuffleGrouping("kafkaReader");

		// builder.setSpout("testSpout", new DpsTaskSpoutTest(), numberOfExecutors).setNumTasks(numberOfTasks);

		builder.setBolt("retrieveFileBolt", retrieveFileBolt, numberOfExecutors).setNumTasks(numberOfTasks)
				.shuffleGrouping("parseKafkaInput");

		builder.setBolt("xsltTransformationBolt", new XsltBolt(), numberOfExecutors)
				.setNumTasks(numberOfTasks).shuffleGrouping("retrieveFileBolt");

		builder.setBolt("writeRecordBolt", writeRecordBolt, numberOfExecutors).setNumTasks(numberOfTasks)
				.shuffleGrouping("xsltTransformationBolt");

		builder.setBolt("progressBolt", progressBolt, 1).setNumTasks(1)
				.shuffleGrouping("writeRecordBolt");
		// END OF TOPOLOGY STRUCTURE

		return builder.createTopology();
	}

	public static void main(String[] args) throws Exception {

		String kafkaZk = args[0];
		XSLTTopology kafkaSpoutTestTopology = new XSLTTopology(kafkaZk);
		Config config = new Config();
		config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 2000);

		Map<String, String> kafkaMetricsConfig = new HashMap<String, String>();
		kafkaMetricsConfig.put(KafkaMetricsConsumer.KAFKA_BROKER_KEY,
				kafkaBroker);
		kafkaMetricsConfig
				.put(KafkaMetricsConsumer.KAFKA_TOPIC_KEY, kafkaTopic);
		config.registerMetricsConsumer(KafkaMetricsConsumer.class,
				kafkaMetricsConfig, 60);

		StormTopology stormTopology = kafkaSpoutTestTopology.buildTopology();

		if (args != null && args.length > 1) {
			String dockerIp = args[1];
			String name = args[2];
			config.setNumWorkers(4);
			config.setMaxTaskParallelism(1);
			config.put(Config.NIMBUS_THRIFT_PORT, 6627);
			config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
			config.put(Config.NIMBUS_HOST, dockerIp);
			config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(kafkaZk));
			StormSubmitter.submitTopology(name, config, stormTopology);
		} else {
			config.setNumWorkers(4);
			config.setMaxTaskParallelism(1);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("XSLTTopology", config, stormTopology);
		}
	}
}
