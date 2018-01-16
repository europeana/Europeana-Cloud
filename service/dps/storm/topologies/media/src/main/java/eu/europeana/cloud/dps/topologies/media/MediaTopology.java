package eu.europeana.cloud.dps.topologies.media;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.shade.org.yaml.snakeyaml.Yaml;
import org.apache.storm.shade.org.yaml.snakeyaml.constructor.SafeConstructor;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.europeana.cloud.dps.topologies.media.support.DummySpout;
import eu.europeana.cloud.dps.topologies.media.support.StatsTupleData;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys;
import eu.europeana.cloud.service.dps.storm.utils.TopologyHelper;

public class MediaTopology {
	
	private static final Logger logger = LoggerFactory.getLogger(MediaTopology.class);
	
	private static Config conf;
	
	public static void main(String[] args)
			throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		
		loadConfig();
		
		final boolean isTest = args.length > 0;
		
		TopologyBuilder builder = new TopologyBuilder();
		String topologyName = (String) conf.get(TopologyPropertyKeys.TOPOLOGY_NAME);
		
		if (isTest) {
			builder.setSpout("source", new DummySpout(), 1);
		} else {
			ZkHosts brokerHosts = new ZkHosts((String) conf.get(TopologyPropertyKeys.INPUT_ZOOKEEPER_ADDRESS));
			SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, topologyName, "", "storm");
			kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
			kafkaConfig.ignoreZkOffsets = true;
			kafkaConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
			builder.setSpout("spout", new KafkaSpout(kafkaConfig), 1);
			builder.setBolt("source", new DataSetReaderBolt(), 1).shuffleGrouping("spout");
		}
		
		builder.setBolt("downloadBolt", new DownloadBolt(),
				(int) conf.get("MEDIATOPOLOGY_PARALLEL_HINT_DOWNLOAD"))
				.shuffleGrouping("source");
		builder.setBolt("processingBolt", new ProcessingBolt(),
				(int) conf.get("MEDIATOPOLOGY_PARALLEL_HINT_PROCESSING"))
				.shuffleGrouping("downloadBolt");
		
		builder.setBolt("statsBolt", new StatsBolt(), 1)
				.shuffleGrouping("downloadBolt", StatsTupleData.STREAM_ID)
				.shuffleGrouping("processingBolt", StatsTupleData.STREAM_ID);
		
		builder.setBolt(TopologyHelper.NOTIFICATION_BOLT,
				new NotificationBolt((String) conf.get(TopologyPropertyKeys.CASSANDRA_HOSTS),
						(int) conf.get(TopologyPropertyKeys.CASSANDRA_PORT),
						(String) conf.get(TopologyPropertyKeys.CASSANDRA_KEYSPACE_NAME),
						(String) conf.get(TopologyPropertyKeys.CASSANDRA_USERNAME),
						(String) conf.get(TopologyPropertyKeys.CASSANDRA_PASSWORD)),
				(int) conf.get(TopologyPropertyKeys.NOTIFICATION_BOLT_PARALLEL))
				.setNumTasks((int) conf.get(TopologyPropertyKeys.NOTIFICATION_BOLT_NUMBER_OF_TASKS))
				.fieldsGrouping("statsBolt", AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
						new Fields(NotificationTuple.taskIdFieldName));
		
		if (isTest) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(topologyName, conf, builder.createTopology());
			Utils.sleep(600000);
			cluster.killTopology(topologyName);
			cluster.shutdown();
		} else {
			StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
		}
		
	}
	
	private static void loadConfig() {
		conf = new Config();
		Yaml yamlConf = new Yaml(new SafeConstructor());
		String configFileName = "media-topology-config.yaml";
		try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(configFileName)) {
			conf.putAll((Map) yamlConf.load(is));
		} catch (IOException e) {
			throw new RuntimeException("Built in config could not be loaded: " + configFileName, e);
		}
		try (InputStream is = new FileInputStream(configFileName)) {
			conf.putAll((Map) yamlConf.load(is));
		} catch (IOException e) {
			logger.warn("Could not load custom config file, using defaults");
			logger.debug("Custom config load problem", e);
		}
	}
}
