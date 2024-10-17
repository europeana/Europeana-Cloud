package eu.europeana.cloud.service.dps.storm.topologies.properties;


/**
 * Class containing topology property file keys
 */
public final class TopologyPropertyKeys {

  public static final String TOPOLOGY_NAME = "TOPOLOGY_NAME";
  public static final String WORKER_COUNT = "WORKER_COUNT";
  public static final String THRIFT_PORT = "THRIFT_PORT";

  public static final String BOOTSTRAP_SERVERS = "BOOTSTRAP_SERVERS";
  public static final String TOPICS = "TOPICS";
  public static final String MCS_URL = "MCS_URL";
  public static final String TOPOLOGY_USER_NAME = "TOPOLOGY_USER_NAME";
  public static final String TOPOLOGY_USER_PASSWORD = "TOPOLOGY_USER_PASSWORD";
  public static final String CASSANDRA_HOSTS = "CASSANDRA_HOSTS";
  public static final String CASSANDRA_PORT = "CASSANDRA_PORT";
  public static final String CASSANDRA_KEYSPACE_NAME = "CASSANDRA_KEYSPACE_NAME";
  public static final String CASSANDRA_USERNAME = "CASSANDRA_USERNAME";
  public static final String CASSANDRA_SECRET_TOKEN = "CASSANDRA_PASSWORD";
  public static final String RETRIEVE_FILE_BOLT_PARALLEL = "RETRIEVE_FILE_BOLT_PARALLEL";
  public static final String XSLT_BOLT_PARALLEL = "XSLT_BOLT_PARALLEL";
  public static final String WRITE_BOLT_PARALLEL = "WRITE_BOLT_PARALLEL";
  public static final String REVISION_WRITER_BOLT_PARALLEL = "REVISION_WRITER_BOLT_PARALLEL";
  public static final String VALIDATION_BOLT_PARALLEL = "VALIDATION_BOLT_PARALLEL";
  public static final String INDEXING_BOLT_PARALLEL = "INDEXING_BOLT_PARALLEL";
  public static final String STATISTICS_BOLT_PARALLEL = "STATISTICS_BOLT_PARALLEL";
  public static final String NOTIFICATION_BOLT_PARALLEL = "NOTIFICATION_BOLT_PARALLEL";
  public static final String MAX_TASK_PARALLELISM = "MAX_TASK_PARALLELISM";
  public static final String RETRIEVE_FILE_BOLT_NUMBER_OF_TASKS = "RETRIEVE_FILE_BOLT_NUMBER_OF_TASKS";
  public static final String XSLT_BOLT_NUMBER_OF_TASKS = "XSLT_BOLT_NUMBER_OF_TASKS";
  public static final String WRITE_BOLT_NUMBER_OF_TASKS = "WRITE_BOLT_NUMBER_OF_TASKS";
  public static final String REVISION_WRITER_BOLT_NUMBER_OF_TASKS = "REVISION_WRITER_BOLT_NUMBER_OF_TASKS";
  public static final String NOTIFICATION_BOLT_NUMBER_OF_TASKS = "NOTIFICATION_BOLT_NUMBER_OF_TASKS";
  public static final String NIMBUS_SEEDS = "NIMBUS_SEEDS";
  public static final String VALIDATION_BOLT_NUMBER_OF_TASKS = "VALIDATION_BOLT_NUMBER_OF_TASKS";
  public static final String INDEXING_BOLT_NUMBER_OF_TASKS = "INDEXING_BOLT_NUMBER_OF_TASKS";
  public static final String STATISTICS_BOLT_NUMBER_OF_TASKS = "STATISTICS_BOLT_NUMBER_OF_TASKS";
  public static final String DUPLICATES_BOLT_PARALLEL = "DUPLICATES_BOLT_PARALLEL";
  public static final String DUPLICATES_BOLT_NUMBER_OF_TASKS = "DUPLICATES_BOLT_NUMBER_OF_TASKS";
  public static final String EDM_OBJECT_PROCESSOR_BOLT_PARALLEL = "EDM_OBJECT_PROCESSOR_BOLT_PARALLEL";
  public static final String EDM_OBJECT_PROCESSOR_BOLT_NUMBER_OF_TASKS = "EDM_OBJECT_PROCESSOR_BOLT_NUMBER_OF_TASKS";
  public static final String PARSE_FILE_BOLT_BOLT_NUMBER_OF_TASKS = "PARSE_FILE_BOLT_BOLT_NUMBER_OF_TASKS";
  public static final String RESOURCE_PROCESSING_BOLT_NUMBER_OF_TASKS = "RESOURCE_PROCESSING_BOLT_NUMBER_OF_TASKS";
  public static final String EDM_ENRICHMENT_BOLT_NUMBER_OF_TASKS = "EDM_ENRICHMENT_BOLT_NUMBER_OF_TASKS";
  public static final String PARSE_FILE_BOLT_PARALLEL = "PARSE_FILE_BOLT_PARALLEL";
  public static final String RESOURCE_PROCESSING_BOLT_PARALLEL = "RESOURCE_PROCESSING_BOLT_PARALLEL";
  public static final String EDM_ENRICHMENT_BOLT_PARALLEL = "EDM_ENRICHMENT_BOLT_PARALLEL";
  public static final String MESSAGE_TIMEOUT_IN_SECONDS = "MESSAGE_TIMEOUT_IN_SECONDS";
  public static final String MAX_SPOUT_PENDING = "MAX_SPOUT_PENDING";
  public static final String MAX_POLL_RECORDS = "MAX_POLL_RECORDS";
  public static final String FETCH_MAX_BYTES = "FETCH_MAX_BYTES";
  public static final String SPOUT_SLEEP_MS = "SPOUT_SLEEP_MS";
  public static final String SPOUT_SLEEP_EVERY_N_IDLE_ITERATIONS = "SPOUT_SLEEP_EVERY_N_IDLE_ITERATIONS";

  public static final String LINK_CHECK_BOLT_PARALLEL = "LINK_CHECK_BOLT_PARALLEL";
  public static final String LINK_CHECK_BOLT_NUMBER_OF_TASKS = "LINK_CHECK_BOLT_NUMBER_OF_TASKS";

  public static final String DEPUBLICATION_BOLT_PARALLEL = "DEPUBLICATION_BOLT_PARALLEL";
  public static final String DEPUBLICATION_BOLT_NUMBER_OF_TASKS = "DEPUBLICATION_BOLT_NUMBER_OF_TASKS";

  public static final String AWS_CREDENTIALS_ACCESSKEY = "AWS_CREDENTIALS_ACCESSKEY";
  public static final String AWS_CREDENTIALS_SECRETKEY = "AWS_CREDENTIALS_SECRETKEY";
  public static final String AWS_CREDENTIALS_ENDPOINT = "AWS_CREDENTIALS_ENDPOINT";
  public static final String AWS_CREDENTIALS_BUCKET = "AWS_CREDENTIALS_BUCKET";

  //Enrichment parameters
  public static final String ENRICHMENT_BOLT_NUMBER_OF_TASKS = "ENRICHMENT_BOLT_NUMBER_OF_TASKS";
  public static final String ENRICHMENT_BOLT_PARALLEL = "ENRICHMENT_BOLT_PARALLEL";
  public static final String DEREFERENCE_SERVICE_URL = "DEREFERENCE_SERVICE_URL";
  public static final String ENRICHMENT_ENTITY_MANAGEMENT_URL = "ENTITY_MANAGEMENT_URL";
  public static final String ENRICHMENT_ENTITY_API_URL = "ENTITY_API_URL";
  public static final String ENRICHMENT_ENTITY_API_KEY = "ENTITY_API_KEY";

  //Normalization parameters
  public static final String NORMALIZATION_BOLT_NUMBER_OF_TASKS = "NORMALIZATION_BOLT_NUMBER_OF_TASKS";
  public static final String NORMALIZATION_BOLT_PARALLEL = "NORMALIZATION_BOLT_PARALLEL";

  //OAI-Parameters
  public static final String RECORD_HARVESTING_BOLT_NUMBER_OF_TASKS = "RECORD_HARVESTING_BOLT_NUMBER_OF_TASKS";
  public static final String RECORD_HARVESTING_BOLT_PARALLEL = "RECORD_HARVESTING_BOLT_PARALLEL";
  public static final String UIS_URL = "UIS_URL";

  public static final String DEFAULT_MAXIMUM_PARALLELIZATION = "DEFAULT_MAXIMUM_PARALLELIZATION";

  private TopologyPropertyKeys() {
  }

}
