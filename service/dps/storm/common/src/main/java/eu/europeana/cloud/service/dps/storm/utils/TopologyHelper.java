package eu.europeana.cloud.service.dps.storm.utils;

import org.apache.storm.Config;

import java.util.Arrays;
import java.util.Properties;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.*;
import static java.lang.Integer.parseInt;

/**
 * Created by Tarek on 7/15/2016.
 */
public final class TopologyHelper {
    public static final String SPOUT = "spout";
    public static final String PARSE_TASK_BOLT = "parseTaskBolt";
    public static final String RETRIEVE_FILE_BOLT = "retrieveFileBolt";
    public static final String READ_DATASETS_BOLT = "readDatasetsBolt";
    public static final String READ_DATASET_BOLT = "readDatasetBolt";
    public static final String READ_REPRESENTATION_BOLT = "readRepresentationBolt";
    public static final String IC_BOLT = "icBolt";
    public static final String NOTIFICATION_BOLT = "notificationBolt";
    public static final String WRITE_RECORD_BOLT = "writeRecordBolt";
    public static final String XSLT_BOLT = "XSLT_BOLT";
    public static final String WRITE_TO_DATA_SET_BOLT = "writeToDataSetBolt";
    public static final String REVISION_WRITER_BOLT = "revisionWriterBolt";
    public static final String VALIDATION_BOLT = "validationBolt";
    public static final String STATISTICS_BOLT = "statisticsBolt";
    public static final String ENRICHMENT_BOLT = "enrichmentBolt";

    public static final String IDENTIFIERS_HARVESTING_BOLT = "identifiersHarvestingBolt";
    public static final String RECORD_HARVESTING_BOLT = "recordHarvestingBolt";
    public static final String TASK_SPLITTING_BOLT = "TaskSplittingBolt";

    public static Config configureTopology(Properties topologyProperties) {
        Config config = new Config();
        config.setNumWorkers(parseInt(topologyProperties.getProperty(WORKER_COUNT)));
        config.setMaxTaskParallelism(
                parseInt(topologyProperties.getProperty(MAX_TASK_PARALLELISM)));
        config.put(Config.NIMBUS_THRIFT_PORT,
                parseInt(topologyProperties.getProperty(THRIFT_PORT)));
        config.put(topologyProperties.getProperty(INPUT_ZOOKEEPER_ADDRESS),
                topologyProperties.getProperty(INPUT_ZOOKEEPER_PORT));
        config.put(Config.NIMBUS_SEEDS, Arrays.asList(topologyProperties.getProperty(NIMBUS_SEEDS)));
        config.put(Config.STORM_ZOOKEEPER_SERVERS,
                Arrays.asList(topologyProperties.getProperty(STORM_ZOOKEEPER_ADDRESS)));

        config.put(CASSANDRA_HOSTS, topologyProperties.getProperty(CASSANDRA_HOSTS));
        config.put(CASSANDRA_PORT, topologyProperties.getProperty(CASSANDRA_PORT));
        config.put(CASSANDRA_KEYSPACE_NAME, topologyProperties.getProperty(CASSANDRA_KEYSPACE_NAME));
        config.put(CASSANDRA_USERNAME, topologyProperties.getProperty(CASSANDRA_USERNAME));
        config.put(CASSANDRA_PASSWORD, topologyProperties.getProperty(CASSANDRA_PASSWORD));

        return config;
    }
}
