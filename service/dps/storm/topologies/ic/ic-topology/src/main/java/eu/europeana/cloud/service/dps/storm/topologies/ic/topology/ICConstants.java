package eu.europeana.cloud.service.dps.storm.topologies.ic.topology;

/**
 *
 * */
public class ICConstants {
    public static final String INPUT_ZOOKEEPER_ADDRESS = "iks-kbase.synat.pcss.pl";
    public static final String INPUT_ZOOKEEPER_PORT = "2181";

    // ------ Cassandra ------
    public static final String CASSANDRA_HOSTS = "iks-kbase.synat.pcss.pl"; //cassandra node hosts, comma separated
    public static final Integer CASSANDRA_PORT = 9042;
    public static final String CASSANDRA_KEYSPACE_NAME = "ecloud_dps";
    public static final String CASSANDRA_USERNAME = "ecloud_dps";
    public static final String CASSANDRA_PASSWORD = "password";

    // ------ PARALLELISM HINTS ------
    public static final Integer KAFKA_SPOUT_PARALLEL = 1;
    public static final Integer PARSE_TASKS_BOLT_PARALLEL = 1;
    public static final Integer RETRIEVE_FILE_BOLT_PARALLEL = 1;
    public static final Integer XSLT_BOLT_PARALLEL = 1;
    public static final Integer WRITE_BOLT_PARALLEL = 1;
    public static final Integer END_BOLT_PARALLEL = 1;
    public static final Integer NOTIFICATION_BOLT_PARALLEL = 1;

    public static final Integer NUMBER_OF_TASKS = 1;
    public static final Integer MAX_TASK_PARALLELISM = 1;

    public static final Integer METRICS_CONSUMER_PARALLEL = 1;
}
