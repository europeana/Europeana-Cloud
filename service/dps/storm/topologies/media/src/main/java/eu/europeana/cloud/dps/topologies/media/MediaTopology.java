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
import org.apache.storm.shade.org.yaml.snakeyaml.Yaml;
import org.apache.storm.shade.org.yaml.snakeyaml.constructor.SafeConstructor;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys;
import eu.europeana.cloud.service.dps.storm.utils.TopologyHelper;

public class MediaTopology {
	
	private static final Logger logger = LoggerFactory.getLogger(MediaTopology.class);
	
	public static void main(String[] args)
			throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		
		Config conf = loadConfig();
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("fileUrlSpout", new FileUrlSpout(), 1);
		builder.setBolt("fileDownloadBolt", new FileDownloadBolt(), 10).shuffleGrouping("fileUrlSpout");
		builder.setBolt("statsBolt", new StatsBolt(), 1).shuffleGrouping("fileDownloadBolt");
		
		builder.setBolt(TopologyHelper.NOTIFICATION_BOLT,
				new NotificationBolt((String) conf.get(TopologyPropertyKeys.CASSANDRA_HOSTS),
						(Integer) conf.get(TopologyPropertyKeys.CASSANDRA_PORT),
						(String) conf.get(TopologyPropertyKeys.CASSANDRA_KEYSPACE_NAME),
						(String) conf.get(TopologyPropertyKeys.CASSANDRA_USERNAME),
						(String) conf.get(TopologyPropertyKeys.CASSANDRA_PASSWORD)),
				(Integer) conf.get(TopologyPropertyKeys.NOTIFICATION_BOLT_PARALLEL))
				.setNumTasks((Integer) conf.get(TopologyPropertyKeys.NOTIFICATION_BOLT_NUMBER_OF_TASKS))
				.fieldsGrouping("statsBolt", AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
						new Fields(NotificationTuple.taskIdFieldName));
		
		String topologyName = (String) conf.get(TopologyPropertyKeys.TOPOLOGY_NAME);
		
		if (args.length > 0) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(topologyName, conf, builder.createTopology());
			Utils.sleep(600000);
			cluster.killTopology(topologyName);
			cluster.shutdown();
		} else {
			StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
		}
		
	}

	private static Config loadConfig() {
		Config conf = new Config();
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
		return conf;
	}
}
