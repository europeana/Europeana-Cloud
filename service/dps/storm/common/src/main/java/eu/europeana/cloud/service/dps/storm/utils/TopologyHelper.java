package eu.europeana.cloud.service.dps.storm.utils;

import eu.europeana.cloud.service.dps.DpsRecord;
import eu.europeana.cloud.service.dps.service.kafka.util.DpsRecordDeserializer;
import eu.europeana.cloud.service.dps.storm.spout.ECloudSpout;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.Config;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;

import java.util.Arrays;
import java.util.Properties;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyDefaultsConstants.*;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.*;
import static java.lang.Integer.parseInt;

/**
 * Created by Tarek on 7/15/2016.
 */
public final class TopologyHelper {
    public static final String SPOUT = "spout";
    public static final String RETRIEVE_FILE_BOLT = "retrieveFileBolt";
    public static final String NOTIFICATION_BOLT = "notificationBolt";
    public static final String WRITE_RECORD_BOLT = "writeRecordBolt";
    public static final String XSLT_BOLT = "XSLT_BOLT";
    public static final String WRITE_TO_DATA_SET_BOLT = "writeToDataSetBolt";
    public static final String REVISION_WRITER_BOLT = "revisionWriterBolt";
    public static final String DUPLICATES_DETECTOR_BOLT = "duplicatesDetectorBolt";
    public static final String VALIDATION_BOLT = "validationBolt";
    public static final String INDEXING_BOLT = "indexingBolt";
    public static final String STATISTICS_BOLT = "statisticsBolt";
    public static final String ENRICHMENT_BOLT = "enrichmentBolt";
    public static final String NORMALIZATION_BOLT = "normalizationBolt";
    public static final String RECORD_HARVESTING_BOLT = "recordHarvestingBolt";
    public static final String RECORD_CATEGORIZATION_BOLT = "recordCategorizationBolt";
    public static final String PARSE_FILE_BOLT = "ParseFileBolt";
    public static final String EDM_ENRICHMENT_BOLT = "EDMEnrichmentBolt";
    public static final String EDM_OBJECT_PROCESSOR_BOLT = "EDMObjectProcessorBolt";
    public static final String RESOURCE_PROCESSING_BOLT = "ResourceProcessingBolt";
    public static final String LINK_CHECK_BOLT = "LinkCheckBolt";

    private TopologyHelper() {
    }

    public static Config buildConfig(Properties topologyProperties) {
        return buildConfig(topologyProperties, false);
    }

    public static Config buildConfig(Properties topologyProperties, boolean staticMode) {
        Config config = new Config();

        if(!staticMode) {
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

            config.put(Config.TOPOLOGY_BACKPRESSURE_ENABLE, true);
        }

        config.setDebug(staticMode);
        config.setMessageTimeoutSecs(getValue(topologyProperties, MESSAGE_TIMEOUT_IN_SECONDS, DEFAULT_TUPLE_PROCESSING_TIME) );

        config.put(CASSANDRA_HOSTS,
                getValue(topologyProperties, CASSANDRA_HOSTS, staticMode ? DEFAULT_CASSANDRA_HOSTS : null) );
        config.put(CASSANDRA_PORT,
                getValue(topologyProperties, CASSANDRA_PORT, staticMode ? DEFAULT_CASSANDRA_PORT : null) );
        config.put(CASSANDRA_KEYSPACE_NAME,
                getValue(topologyProperties, CASSANDRA_KEYSPACE_NAME, staticMode ? DEFAULT_CASSANDRA_KEYSPACE_NAME : null) );
        config.put(CASSANDRA_USERNAME,
                getValue(topologyProperties, CASSANDRA_USERNAME, staticMode ? DEFAULT_CASSANDRA_USERNAME : null) );
        config.put(CASSANDRA_SECRET_TOKEN,
                getValue(topologyProperties, CASSANDRA_SECRET_TOKEN, staticMode ? DEFAULT_CASSANDRA_SECRET_TOKEN : null) );

        config.setMaxSpoutPending(getValue(topologyProperties, MAX_SPOUT_PENDING, DEFAULT_MAX_SPOUT_PENDING));

        return config;
    }

    private static String getValue(Properties properties, String key, String defaultValue) {
        if(properties != null && properties.containsKey(key)) {
            return properties.getProperty(key);
        } else {
            return defaultValue;
        }
    }

    private static int getValue(Properties properties, String key, int defaultValue) {
        if(properties != null && properties.containsKey(key)) {
            return Integer.parseInt(properties.getProperty(key));
        } else {
            return defaultValue;
        }
    }

    public static ECloudSpout createECloudSpout(String topologyName, Properties topologyProperties) {
        return  createECloudSpout(topologyName, topologyProperties, KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE);
    }

    public static ECloudSpout createECloudSpout(String topologyName, Properties topologyProperties, KafkaSpoutConfig.ProcessingGuarantee processingGuarantee) {

        KafkaSpoutConfig.Builder<String, DpsRecord> configBuilder =
                new KafkaSpoutConfig.Builder<String, DpsRecord>(
                        topologyProperties.getProperty(BOOTSTRAP_SERVERS), topologyProperties.getProperty(TOPICS).split(","))
                        .setProcessingGuarantee(processingGuarantee)
                        .setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                        .setProp(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DpsRecordDeserializer.class)
                        .setProp(ConsumerConfig.GROUP_ID_CONFIG, topologyName)
                        .setProp(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, getValue(topologyProperties, MAX_POLL_RECORDS, DEFAULT_MAX_POLL_RECORDS))
                        .setProp(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, getValue(topologyProperties, FETCH_MAX_BYTES, DEFAULT_FETCH_MAX_BYTES))
                        .setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_LATEST);

        return new ECloudSpout(
                topologyName,
                configBuilder.build(),
                topologyProperties.getProperty(CASSANDRA_HOSTS),
                Integer.parseInt(topologyProperties.getProperty(CASSANDRA_PORT)),
                topologyProperties.getProperty(CASSANDRA_KEYSPACE_NAME),
                topologyProperties.getProperty(CASSANDRA_USERNAME),
                topologyProperties.getProperty(CASSANDRA_SECRET_TOKEN));
    }
}
