package eu.europeana.cloud.service.dps.kafka;

import java.util.Arrays;

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
import eu.europeana.cloud.service.dps.storm.bolts.ProcessDPSInputMessageBolt;

public class EcloudXSLTTopology {
	public static final Logger LOG = LoggerFactory.getLogger(EcloudXSLTTopology.class);

	private final BrokerHosts brokerHosts;

	public EcloudXSLTTopology(String kafkaZookeeper) {
		brokerHosts = new ZkHosts(kafkaZookeeper);
	}

	public StormTopology buildTopology() {
		SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, "franco_maria_topic", "", "storm");
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("kafkaReader", new KafkaSpout(kafkaConfig), 10);
		builder.setBolt("processDPSmessage", new ProcessDPSInputMessageBolt()).shuffleGrouping("kafkaReader");
		// builder.setBolt("loadRecord", new ...Bolt()).shuffleGrouping("processDPSmessage");
		// builder.setBolt("tranformRecord", new ...Bolt()).shuffleGrouping("loadRecord");
		// builder.setBolt("writeRecord", new ...Bolt()).shuffleGrouping("transformRecord");
		return builder.createTopology();
	}

	public static void main(String[] args) throws Exception {

		// String kafkaZk = args[0];
		String kafkaZk = "ecloud.eanadev.org:2181";
		EcloudXSLTTopology kafkaSpoutTestTopology = new EcloudXSLTTopology(kafkaZk);
		Config config = new Config();
		config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 2000);

		StormTopology stormTopology = kafkaSpoutTestTopology.buildTopology();
		if (args != null && args.length > 1) {
			String name = args[0];
			String dockerIp = args[1];
			config.setNumWorkers(2);
			config.setMaxTaskParallelism(5);
			config.put(Config.NIMBUS_HOST, dockerIp);
			config.put(Config.NIMBUS_THRIFT_PORT, 6627);
			config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
			config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(dockerIp));
			StormSubmitter.submitTopology(name, config, stormTopology);
		} else {
			config.setNumWorkers(2);
			config.setMaxTaskParallelism(2);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("kafka", config, stormTopology);
		}
	}
}
