package eu.europeana.cloud.service.dps.storm.topologies.properties;

public class TopologyDefaultsConstants {
    private TopologyDefaultsConstants() {
    }

    public static final int DEFAULT_TUPLE_PROCESSING_TIME = 5 * 60; // 5min

    public static final String DEFAULT_CASSANDRA_HOSTS = "localhost";
    public static final String DEFAULT_CASSANDRA_PORT = "9042";
    public static final String DEFAULT_CASSANDRA_KEYSPACE_NAME = "ecloud_dps";
    public static final String DEFAULT_CASSANDRA_USERNAME = "cassandra";
    public static final String DEFAULT_CASSANDRA_SECRET_TOKEN = "cassandra";


    public static final String DEFAULT_MCS_URL = "http://localhost:8080/mcs";
    public static final String DEFAULT_UIS_URL = "http://localhost:8080/uis";
    public static final String DEFAULT_ECLOUD_MCS_USERNAME = "admin";
    public static final String DEFAULT_ECLOUD_MCS_SECRET_TOKEN = "admin";

    public static final String DEFAULT_ZOOKEEPER_HOST = "localhost";
    public static final String DEFAULT_KAFKA_HOST = "localhost";

}
