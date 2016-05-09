package eu.europeana.cloud.service.dps.storm.topologies.xslt;

/**
 *
 * @author Franco Maria Nardini <francomaria.nardini@isti.cnr.it>
 */
public class XSLTConstants 
{
    public static final String KAFKA_INPUT_TOPIC = "poland_cloud_2";
    public static final String INPUT_ZOOKEEPER_ADDRESS = "146.48.82.220";
    public static final String INPUT_ZOOKEEPER_PORT = "2181";
    public static final String ZOOKEEPER_ROOT = "/" + KAFKA_INPUT_TOPIC; 
    
    public static final String KAFKA_METRICS_TOPIC = "storm_metrics_topic";
    public static final String KAFKA_METRICS_BROKER = "146.48.82.220:9092";
    
    public static final String MCS_URL = "https://gillenia.man.poznan.pl/api/";
    public static final String USERNAME = "xyz";
    public static final String PASSWORD = "xyz";
     
    // ------ Cassandra ------
    public static final String CASSANDRA_HOSTS = "tokaji.isti.cnr.it"; //cassandra node hosts, comma separated
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
