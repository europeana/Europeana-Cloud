package eu.europeana.cloud.service.dps.storm.topologies.properties;


public final class TopologyPropertyKeys {

    public TopologyPropertyKeys() {

    }

    public static final String TOPOLOGY_NAME = "TOPOLOGY_NAME";
    public static final String WORKER_COUNT = "WORKER_COUNT";
    public static final String THRIFT_PORT = "THRIFT_PORT";

    public static final String INPUT_ZOOKEEPER_ADDRESS = "INPUT_ZOOKEEPER_ADDRESS";
    public static final String INPUT_ZOOKEEPER_PORT = "INPUT_ZOOKEEPER_PORT";

    public static final String STORM_ZOOKEEPER_ADDRESS = "STORM_ZOOKEEPER_ADDRESS";
    public static final String MCS_URL = "MCS_URL";
    public static final String CASSANDRA_HOSTS = "CASSANDRA_HOSTS";
    public static final String CASSANDRA_PORT = "CASSANDRA_PORT";
    public static final String CASSANDRA_KEYSPACE_NAME = "CASSANDRA_KEYSPACE_NAME";
    public static final String CASSANDRA_USERNAME = "CASSANDRA_USERNAME";
    public static final String CASSANDRA_PASSWORD = "CASSANDRA_PASSWORD";
    public static final String KAFKA_SPOUT_PARALLEL = "KAFKA_SPOUT_PARALLEL";
    public static final String PARSE_TASKS_BOLT_PARALLEL = "PARSE_TASKS_BOLT_PARALLEL";
    public static final String RETRIEVE_FILE_BOLT_PARALLEL = "RETRIEVE_FILE_BOLT_PARALLEL";
    public static final String XSLT_BOLT_PARALLEL = "XSLT_BOLT_PARALLEL";
    public static final String IC_BOLT_PARALLEL = "IC_BOLT_PARALLEL";
    public static final String WRITE_BOLT_PARALLEL = "WRITE_BOLT_PARALLEL";
    public static final String REVISION_WRITER_BOLT_PARALLEL = "REVISION_WRITER_BOLT_PARALLEL";

    public static final String READ_DATASETS_BOLT_PARALLEL = "READ_DATASETS_BOLT_PARALLEL";
    public static final String READ_DATASET_BOLT_PARALLEL = "READ_DATASET_BOLT_PARALLEL";
    public static final String READ_REPRESENTATION_BOLT_PARALLEL = "READ_REPRESENTATION_BOLT_PARALLEL";
    public static final String NOTIFICATION_BOLT_PARALLEL = "NOTIFICATION_BOLT_PARALLEL";
    public static final String NUMBER_OF_TASKS = "NUMBER_OF_TASKS";
    public static final String MAX_TASK_PARALLELISM = "MAX_TASK_PARALLELISM";
    public static final String KAFKA_SPOUT_NUMBER_OF_TASKS = "KAFKA_SPOUT_NUMBER_OF_TASKS";
    public static final String PARSE_TASKS_BOLT_NUMBER_OF_TASKS = "PARSE_TASKS_BOLT_NUMBER_OF_TASKS";
    public static final String RETRIEVE_FILE_BOLT_NUMBER_OF_TASKS = "RETRIEVE_FILE_BOLT_NUMBER_OF_TASKS";
    public static final String XSLT_BOLT_NUMBER_OF_TASKS = "XSLT_BOLT_NUMBER_OF_TASKS";
    public static final String WRITE_BOLT_NUMBER_OF_TASKS = "WRITE_BOLT_NUMBER_OF_TASKS";
    public static final String Revision_WRITER_BOLT_NUMBER_OF_TASKS = "Revision_WRITER_BOLT_NUMBER_OF_TASKS";
    public static final String READ_DATASETS_BOLT_NUMBER_OF_TASKS = "READ_DATASETS_BOLT_NUMBER_OF_TASKS";
    public static final String READ_DATASET_BOLT_NUMBER_OF_TASKS = "READ_DATASET_BOLT_NUMBER_OF_TASKS";
    public static final String READ_REPRESENTATION_BOLT_NUMBER_OF_TASKS = "READ_REPRESENTATION_BOLT_NUMBER_OF_TASKS";
    public static final String NOTIFICATION_BOLT_NUMBER_OF_TASKS = "NOTIFICATION_BOLT_NUMBER_OF_TASKS";
    public static final String ADD_TO_DATASET_BOLT_NUMBER_OF_TASKS = "ADD_TO_DATASET_BOLT_NUMBER_OF_TASKS";
    public static final String IC_BOLT_NUMBER_OF_TASKS = "IC_BOLT_NUMBER_OF_TASKS";
    public static final String NIMBUS_SEEDS = "NIMBUS_SEEDS";
    public static final String ADD_TO_DATASET_BOLT_PARALLEL = "ADD_TO_DATASET_BOLT_PARALLEL";

    //OAI-Parameters
    public static final String TASK_SPLITTING_BOLT_BOLT_NUMBER_OF_TASKS = "TASK_SPLITTING_BOLT_BOLT_NUMBER_OF_TASKS";
    public static final String TASK_SPLITTING_BOLT_PARALLEL = "TASK_SPLITTING_BOLT_PARALLEL";
    public static final String RECORD_HARVESTING_BOLT_NUMBER_OF_TASKS = "RECORD_HARVESTING_BOLT_NUMBER_OF_TASKS";
    public static final String RECORD_HARVESTING_BOLT_PARALLEL = "RECORD_HARVESTING_BOLT_PARALLEL";
    public static final String IDENTIFIERS_HARVESTING_BOLT_NUMBER_OF_TASKS = "IDENTIFIERS_HARVESTING_BOLT_NUMBER_OF_TASKS";
    public static final String IDENTIFIERS_HARVESTING_BOLT_PARALLEL = "IDENTIFIERS_HARVESTING_BOLT_PARALLEL";
    public static final String UIS_URL = "UIS_URL";
    public static final String INTERVAL = "INTERVAL";

}
