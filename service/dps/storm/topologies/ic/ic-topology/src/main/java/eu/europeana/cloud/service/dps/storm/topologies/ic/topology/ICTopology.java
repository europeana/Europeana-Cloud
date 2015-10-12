package eu.europeana.cloud.service.dps.storm.topologies.ic.topology;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import eu.europeana.cloud.service.dps.storm.ProgressBolt;
import eu.europeana.cloud.service.dps.storm.io.ReadFileBolt;
import eu.europeana.cloud.service.dps.storm.io.WriteRecordBolt;
import eu.europeana.cloud.service.dps.storm.kafka.KafkaParseTaskBolt;
import eu.europeana.cloud.service.dps.storm.topologies.ic.topology.bolt.IcBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.*;

import java.util.Arrays;

/**
 * This is the Image conversion topology . The topology reads
 * from the cloud, apply Kakadu conversion to each record which was read and save it back to the cloud.
 * 
 * The topology takes some parameters. When deployed in distributed mode:
 * 
 * args[0] is the name of the topology;
 * args[1] is the IP of the Storm Nimbus machine;
 * 
 * args[2] is the IP of the zookeeper machine for storm
 * args[3] is the IP of the zookeeper machine for the dps
 * 
 * args[4] is the Kafka topic where the xslt topology is listening from
 *
 * args[5] is the address of the MCS service
 * args[6] is the MCS username;
 * args[7] is the MCS password;
 * 
 */
public class ICTopology {

	private final int numberOfExecutors = 16;
	private final int numberOfTasks = 16;

	private final static int WORKER_COUNT = 8;
	private final static int TASK_PARALLELISM = 2;
	private final static int THRIFT_PORT = 6627;
	private final static int ZK_PORT = 2181;

	public static final Logger LOGGER = LoggerFactory.getLogger(ICTopology.class);

	private final BrokerHosts brokerHosts;

	public ICTopology(String kafkaZkAddress) {
		brokerHosts = new ZkHosts(kafkaZkAddress);
	}

	public StormTopology buildTopology(String dpsZkAddress, String icTopic,
			String ecloudMcsAddress, String username, String password) {

		ReadFileBolt retrieveFileBolt = new ReadFileBolt(ecloudMcsAddress, username, password);
		WriteRecordBolt writeRecordBolt = new WriteRecordBolt(ecloudMcsAddress, username, password);
		
		ProgressBolt progressBolt = new ProgressBolt(dpsZkAddress);

		SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, icTopic, "", "storm");
		kafkaConfig.forceFromStart = true;
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		TopologyBuilder builder = new TopologyBuilder();
		
		KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);

		// TOPOLOGY STRUCTURE!
		// 1 executor, i.e., 1 thread.
		// 1 task per executor
		builder.setSpout("kafkaReader", kafkaSpout, 1)
				.setNumTasks(numberOfTasks);

		builder.setBolt("parseKafkaInput", new KafkaParseTaskBolt(),
				numberOfExecutors).setNumTasks(numberOfTasks)
				.shuffleGrouping("kafkaReader");

		builder.setBolt("retrieveFileBolt", retrieveFileBolt, numberOfExecutors)
				.setNumTasks(numberOfTasks).shuffleGrouping("parseKafkaInput");

		builder.setBolt("imageConversionBolt", new IcBolt(),
				numberOfExecutors).setNumTasks(numberOfTasks)
				.shuffleGrouping("retrieveFileBolt");

		builder.setBolt("writeRecordBolt", writeRecordBolt, numberOfExecutors)
				.setNumTasks(numberOfTasks)
				.shuffleGrouping("imageConversionBolt");

		builder.setBolt("progressBolt", progressBolt, 1).setNumTasks(1)
				.shuffleGrouping("writeRecordBolt");
		// END OF TOPOLOGY STRUCTURE

		return builder.createTopology();
	}

	public static void main(String[] args) throws Exception {

		Config config = new Config();
		config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 2000);

		if (args != null && args.length > 7) {

			String topologyName = args[0];
			String nimbusHost = args[1];
			String stormZookeeper = args[2];
			String dpsZookeeper = args[3];
			String kafkaTopic = args[4];
			String ecloudMcsAddress = args[5];
			String username = args[6];
			String password = args[7];

			ICTopology kafkaSpoutTestTopology = new ICTopology(dpsZookeeper);
			
			StormTopology stormTopology = kafkaSpoutTestTopology.buildTopology(dpsZookeeper,
					kafkaTopic, ecloudMcsAddress, username, password);

			config.setNumWorkers(WORKER_COUNT);
			config.setMaxTaskParallelism(TASK_PARALLELISM);
			config.put(Config.NIMBUS_THRIFT_PORT, THRIFT_PORT);
			config.put(Config.STORM_ZOOKEEPER_PORT, ZK_PORT);
			config.put(Config.NIMBUS_HOST, nimbusHost);
			config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(stormZookeeper));
			StormSubmitter.submitTopology(topologyName, config, stormTopology);
		}
	}
}
