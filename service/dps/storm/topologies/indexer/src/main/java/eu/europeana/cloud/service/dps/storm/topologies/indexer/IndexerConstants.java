package eu.europeana.cloud.service.dps.storm.topologies.indexer;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class IndexerConstants 
{
    public static final String KAFKA_INPUT_TOPIC = "extraction_topic";
    public static final String INPUT_ZOOKEEPER = "192.168.47.129:2181";
    public static final String ZOOKEEPER_ROOT = "/" + KAFKA_INPUT_TOPIC; 
    
    public static final String KAFKA_METRICS_TOPIC = "storm_metrics_topic";
    public static final String KAFKA_METRICS_BROKER = "192.168.47.129:9093";
    
    public static final String MCS_URL = "https://loheria.man.poznan.pl/api";
    public static final String USERNAME = "pavel_kefurt";
    public static final String PASSWORD = "ieNgio9a";
     
    public static final String ELASTICSEARCH_ADDRESSES = "192.168.47.129:9300";
    public static final String SOLR_ADDRESSES = "http://192.168.47.129:8983/solr";
    
    // ------ Cassandra ------
    public static final String CASSANDRA_HOSTS = "192.168.47.129"; //cassandra node hosts, comma separated
    public static final Integer CASSANDRA_PORT = 9042;
    public static final String CASSANDRA_KEYSPACE_NAME = "ecloud_dps";
    public static final String CASSANDRA_USERNAME = "ecloud_dps";
    public static final String CASSANDRA_PASSWORD = "password";
    
    // ------ INDEX BOLT ------
    public static final Integer CACHE_SIZE = 8;
    
    // ------ PARALLELISM HINTS ------
    public static final Integer KAFKA_SPOUT_PARALLEL = 1;
    public static final Integer PARSE_TASKS_BOLT_PARALLEL = 1;
    public static final Integer FILE_BOLT_PARALLEL = 1;
    public static final Integer INDEX_BOLT_PARALLEL = 1;
    public static final Integer END_BOLT_PARALLEL = 1;
    public static final Integer NOTIFICATION_BOLT_PARALLEL = 1;
    
    public static final Integer METRICS_CONSUMER_PARALLEL = 1;
}
