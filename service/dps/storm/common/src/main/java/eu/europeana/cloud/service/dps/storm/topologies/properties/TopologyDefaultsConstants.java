package eu.europeana.cloud.service.dps.storm.topologies.properties;

public class TopologyDefaultsConstants {
    private TopologyDefaultsConstants() {
    }

    public static final int DEFAULT_TUPLE_PROCESSING_TIME = 30 * 60; // 30min

    public static final String DEFAULT_CASSANDRA_HOSTS = "localhost";
    public static final String DEFAULT_CASSANDRA_PORT = "9042";
    public static final String DEFAULT_CASSANDRA_KEYSPACE_NAME = "ecloud_dps";
    public static final String DEFAULT_CASSANDRA_USERNAME = "cassandra";
    public static final String DEFAULT_CASSANDRA_SECRET_TOKEN = "cassandra";
}
