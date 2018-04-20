package eu.europeana.cloud.dps.topologies.media.support;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.shade.org.yaml.snakeyaml.Yaml;
import org.apache.storm.shade.org.yaml.snakeyaml.constructor.SafeConstructor;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.RevisionServiceClient;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys;

public class Util {
	
	private static final Logger logger = LoggerFactory.getLogger(Util.class);
	
	public static DataSetServiceClient getDataSetServiceClient(Map<String, Object> config, DpsTask task) {
		DataSetServiceClient client = new DataSetServiceClient((String) config.get(TopologyPropertyKeys.MCS_URL));
		client.useAuthorizationHeader(task.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER));
		return client;
	}
	
	public static FileServiceClient getFileServiceClient(Map<String, Object> config, DpsTask task) {
		FileServiceClient client = new FileServiceClient((String) config.get(TopologyPropertyKeys.MCS_URL));
		client.useAuthorizationHeader(task.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER));
		return client;
	}
	
	public static RecordServiceClient getRecordServiceClient(Map<String, Object> config, DpsTask task) {
		RecordServiceClient client = new RecordServiceClient((String) config.get(TopologyPropertyKeys.MCS_URL));
		client.useAuthorizationHeader(task.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER));
		return client;
	}
	
	public static RevisionServiceClient getRevisionServiceClient(Map<String, Object> config, DpsTask task) {
		RevisionServiceClient client = new RevisionServiceClient((String) config.get(TopologyPropertyKeys.MCS_URL));
		client.useAuthorizationHeader(task.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER));
		return client;
	}
	
	public static CassandraConnectionProvider getCassandraConnectionProvider(Map<String, Object> config) {
		String hosts = (String) config.get(TopologyPropertyKeys.CASSANDRA_HOSTS);
		int port = (int) (long) config.get(TopologyPropertyKeys.CASSANDRA_PORT);
		String keyspace = (String) config.get(TopologyPropertyKeys.CASSANDRA_KEYSPACE_NAME);
		String username = (String) config.get(TopologyPropertyKeys.CASSANDRA_USERNAME);
		String password = (String) config.get(TopologyPropertyKeys.CASSANDRA_SECRET_TOKEN);
		return CassandraConnectionProviderSingleton.getCassandraConnectionProvider(hosts, port, keyspace, username,
				password);
	}
	
	public static SpoutConfig getKafkaSpoutConfig(Config conf) {
		String topologyName = (String) conf.get(TopologyPropertyKeys.TOPOLOGY_NAME);
		ZkHosts brokerHosts = new ZkHosts((String) conf.get(TopologyPropertyKeys.INPUT_ZOOKEEPER_ADDRESS));
		SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, topologyName, "", "storm");
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		kafkaConfig.ignoreZkOffsets = true;
		kafkaConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
		return kafkaConfig;
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static Config loadConfig() {
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
