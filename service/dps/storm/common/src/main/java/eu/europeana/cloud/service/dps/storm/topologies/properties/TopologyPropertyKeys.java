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

	public static final String KAFKA_METRICS_TOPIC = "KAFKA_METRICS_TOPIC";
	public static final String KAFKA_METRICS_BROKER = "KAFKA_METRICS_BROKER";

	public static final String MCS_URL = "MCS_URL";
	public static final String MCS_USER_NAME = "MCS_USER_NAME";
	public static final String MCS_USER_PASS = "MCS_USER_PASS";

	public static final String CASSANDRA_HOSTS = "CASSANDRA_HOSTS";
	public static final String CASSANDRA_PORT = "CASSANDRA_PORT";
	public static final String CASSANDRA_KEYSPACE_NAME = "CASSANDRA_KEYSPACE_NAME";
	public static final String CASSANDRA_USERNAME = "CASSANDRA_USERNAME";
	public static final String CASSANDRA_PASSWORD = "CASSANDRA_PASSWORD";

	public static final String KAFKA_SPOUT_PARALLEL = "KAFKA_SPOUT_PARALLEL";
	public static final String PARSE_TASKS_BOLT_PARALLEL = "PARSE_TASKS_BOLT_PARALLEL";
	public static final String RETRIEVE_FILE_BOLT_PARALLEL = "RETRIEVE_FILE_BOLT_PARALLEL";
	public static final String XSLT_BOLT_PARALLEL = "XSLT_BOLT_PARALLEL";
	public static final String WRITE_BOLT_PARALLEL = "WRITE_BOLT_PARALLEL";
	public static final String GRANT_BOLT_PARALLEL = "GRANT_BOLT_PARALLEL";
	public static final String REMOVE_BOLT_PARALLEL = "REMOVE_BOLT_PARALLEL";
	public static final String END_BOLT_PARALLEL = "END_BOLT_PARALLEL";
	public static final String NOTIFICATION_BOLT_PARALLEL = "NOTIFICATION_BOLT_PARALLEL";

	public static final String NUMBER_OF_TASKS = "NUMBER_OF_TASKS";
	public static final String MAX_TASK_PARALLELISM = "MAX_TASK_PARALLELISM";
	public static final String METRICS_CONSUMER_PARALLEL = "METRICS_CONSUMER_PARALLEL";

}
