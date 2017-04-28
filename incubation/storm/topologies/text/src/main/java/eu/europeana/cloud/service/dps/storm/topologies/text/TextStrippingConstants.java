package eu.europeana.cloud.service.dps.storm.topologies.text;

/**
 * constants for text stripping topology.
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class TextStrippingConstants 
{
    public static final String KAFKA_INPUT_TOPIC = "text_stripping";
    public static final String INPUT_ZOOKEEPER = "192.168.47.129:2181";
    public static final String ZOOKEEPER_ROOT = "/" + KAFKA_INPUT_TOPIC; 
    
    public static final String KAFKA_METRICS_TOPIC = "storm_metrics_topic";
    public static final String KAFKA_METRICS_BROKER = "192.168.47.129:9093";

    public static final String MCS_URL = "https://loheria.man.poznan.pl/api";
    public static final String USERNAME = "pavel_kefurt";
    public static final String PASSWORD = "ieNgio9a";
 
    // ------ Cassandra ------
    public static final String CASSANDRA_HOSTS = "192.168.47.129"; //cassandra node hosts, comma separated
    public static final Integer CASSANDRA_PORT = 9042;
    public static final String CASSANDRA_KEYSPACE_NAME = "ecloud_dps";
    public static final String CASSANDRA_USERNAME = "ecloud_dps";
    public static final String CASSANDRA_PASSWORD = "password";
    
    // ------ PARALLELISM HINTS ------
    public static final Integer KAFKA_SPOUT_PARALLEL = 1;
    public static final Integer PARSE_TASKS_BOLT_PARALLEL = 1;
    public static final Integer DATASET_BOLT_PARALLEL = 1;
    public static final Integer FILE_BOLT_PARALLEL = 1;
    public static final Integer EXTRACT_BOLT_PARALLEL = 1;
    public static final Integer STORE_BOLT_PARALLEL = 1;
    public static final Integer END_BOLT_PARALLEL = 1;
    public static final Integer NOTIFICATION_BOLT_PARALLEL = 1;
    
    public static final Integer METRICS_CONSUMER_PARALLEL = 1;
}
