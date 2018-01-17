package eu.europeana.cloud.dps.topologies.media.support;

import java.util.Map;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys;

public class Util {
	
	private static final String CONF_FS_URL = "MEDIATOPOLOGY_FILE_SERVICE_URL";
	private static final String CONF_FS_USER = "MEDIATOPOLOGY_FILE_SERVICE_USER";
	private static final String CONF_FS_PASS = "MEDIATOPOLOGY_FILE_SERVICE_PASSWORD";
	
	public static DataSetServiceClient getDataSetServiceClient(Map<String, Object> config) {
		return new DataSetServiceClient((String) config.get(CONF_FS_URL), (String) config.get(CONF_FS_USER),
				(String) config.get(CONF_FS_PASS));
	}
	
	public static FileServiceClient getFileServiceClient(Map<String, Object> config) {
		return new FileServiceClient((String) config.get(CONF_FS_URL), (String) config.get(CONF_FS_USER),
				(String) config.get(CONF_FS_PASS));
	}
	
	public static RecordServiceClient getRecordServiceClient(Map<String, Object> config) {
		return new RecordServiceClient((String) config.get(CONF_FS_URL), (String) config.get(CONF_FS_USER),
				(String) config.get(CONF_FS_PASS));
	}
	
	public static CassandraConnectionProvider getCassandraConnectionProvider(Map<String, Object> config) {
		String hosts = (String) config.get(TopologyPropertyKeys.CASSANDRA_HOSTS);
		int port = (int) (long) config.get(TopologyPropertyKeys.CASSANDRA_PORT);
		String keyspace = (String) config.get(TopologyPropertyKeys.CASSANDRA_KEYSPACE_NAME);
		String username = (String) config.get(TopologyPropertyKeys.CASSANDRA_USERNAME);
		String password = (String) config.get(TopologyPropertyKeys.CASSANDRA_PASSWORD);
		return CassandraConnectionProviderSingleton.getCassandraConnectionProvider(hosts, port, keyspace, username,
				password);
	}
}
