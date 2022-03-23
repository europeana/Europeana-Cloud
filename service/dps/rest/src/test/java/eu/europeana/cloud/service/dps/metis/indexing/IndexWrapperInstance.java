package eu.europeana.cloud.service.dps.metis.indexing;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.service.dps.service.utils.indexing.IndexWrapper;
import eu.europeana.cloud.service.dps.service.utils.indexing.IndexWrapperException;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_HOSTS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_KEYSPACE_NAME;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_PORT;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_SECRET_TOKEN;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_USERNAME;

public class IndexWrapperInstance {

    private static final String INDEXING_PROPERTIES_FILE_NAME = "<PATH_TO_CONFIGURATION_PROJECT>/production/indexing.properties";
    private static final String CASSANDRA_CONNECTION_FILE_NAME = "<PATH_TO_CONFIGURATION_PROJECT>/acceptance/topologies/indexing_topology_config.properties";

    public static IndexWrapper getWrapper() {
        return IndexWrapper.getInstance(loadProperties(INDEXING_PROPERTIES_FILE_NAME));
    }

    private static Properties loadProperties(String fileName) {
        try {
            Properties properties = new Properties();
            InputStream input = new FileInputStream(fileName);
            properties.load(input);
            return properties;
        } catch (Exception e) {
            throw new IndexWrapperException("Unable to read indexing.properties (are you sure that file exists?). Dataset will not  be cleared before indexing.", e);
        }
    }

    public static CassandraConnectionProvider getProvider() {
        Properties properties = loadProperties(CASSANDRA_CONNECTION_FILE_NAME);
        return CassandraConnectionProviderSingleton.getCassandraConnectionProvider(properties.getProperty(CASSANDRA_HOSTS),
                Integer.parseInt(properties.getProperty(CASSANDRA_PORT)),
                properties.getProperty(CASSANDRA_KEYSPACE_NAME),
                properties.getProperty(CASSANDRA_USERNAME),
                properties.getProperty(CASSANDRA_SECRET_TOKEN));
    }

    public static HarvestedRecordsDAO getHarvestedRecordsDAO() {
        return HarvestedRecordsDAO.getInstance(getProvider());
    }

}
