package eu.europeana.cloud.service.dps.storm.utils;

import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.service.dps.DpsRecord;
import eu.europeana.cloud.service.dps.DpsRecordDeserializer;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.metis.indexing.DataSetCleanerParameters;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.spout.ECloudSpout;
import eu.europeana.cloud.service.dps.storm.spout.MediaSpout;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.Config;
import org.apache.storm.daemon.GrouperFactory;
import org.apache.storm.grouping.ShuffleGrouping;
import org.apache.storm.kafka.spout.FirstPollOffsetStrategy;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;

import static eu.europeana.cloud.service.dps.storm.AbstractDpsBolt.NOTIFICATION_STREAM_NAME;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyDefaultsConstants.*;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.*;
import static java.lang.Integer.parseInt;
import static org.apache.storm.Config.TOPOLOGY_KRYO_REGISTER;

/**
 * Created by Tarek on 7/15/2016.
 */
public final class TopologyHelper {
    public static final String SPOUT_NAME_PREFIX = "spout_";
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

        if (!staticMode) {
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
        config.setMessageTimeoutSecs(getValue(topologyProperties, MESSAGE_TIMEOUT_IN_SECONDS, DEFAULT_TUPLE_PROCESSING_TIME));

        config.put(CASSANDRA_HOSTS,
                getValue(topologyProperties, CASSANDRA_HOSTS, staticMode ? DEFAULT_CASSANDRA_HOSTS : null));
        config.put(CASSANDRA_PORT,
                getValue(topologyProperties, CASSANDRA_PORT, staticMode ? DEFAULT_CASSANDRA_PORT : null));
        config.put(CASSANDRA_KEYSPACE_NAME,
                getValue(topologyProperties, CASSANDRA_KEYSPACE_NAME, staticMode ? DEFAULT_CASSANDRA_KEYSPACE_NAME : null));
        config.put(CASSANDRA_USERNAME,
                getValue(topologyProperties, CASSANDRA_USERNAME, staticMode ? DEFAULT_CASSANDRA_USERNAME : null));
        config.put(CASSANDRA_SECRET_TOKEN,
                getValue(topologyProperties, CASSANDRA_SECRET_TOKEN, staticMode ? DEFAULT_CASSANDRA_SECRET_TOKEN : null));

        config.setMaxSpoutPending(getValue(topologyProperties, MAX_SPOUT_PENDING, DEFAULT_MAX_SPOUT_PENDING));

        config.put(TOPOLOGY_KRYO_REGISTER, Arrays.asList(LinkedHashMap.class.getName(),
                OAIPMHHarvestingDetails.class.getName(), Revision.class.getName(), Date.class.getName(),
                DataSetCleanerParameters.class.getName(),ThrottlingShuffleGrouping.class.getName()));

        config.put(Config.TOPOLOGY_SPOUT_WAIT_STRATEGY, FastCancelingSpoutWaitStrategy.class.getName());
        config.put(SPOUT_SLEEP_MS, getValue(topologyProperties, SPOUT_SLEEP_MS, DEFAULT_SPOUT_SLEEP_MS));
        config.put(SPOUT_SLEEP_EVERY_N_IDLE_ITERATIONS, getValue(topologyProperties,
                SPOUT_SLEEP_EVERY_N_IDLE_ITERATIONS, DEFAULT_SPOUT_SLEEP_EVERY_N_IDLE_ITERATIONS));
        return config;
    }

    private static String getValue(Properties properties, String key, String defaultValue) {
        if (properties != null && properties.containsKey(key)) {
            return properties.getProperty(key);
        } else {
            return defaultValue;
        }
    }

    private static int getValue(Properties properties, String key, int defaultValue) {
        if (properties != null && properties.containsKey(key)) {
            return Integer.parseInt(properties.getProperty(key));
        } else {
            return defaultValue;
        }
    }

    public static ECloudSpout createECloudSpout(String topologyName, Properties topologyProperties, String topic) {
        return new ECloudSpout(
                topologyName, topic,
                createKafkaSpoutConfig(topologyName, topologyProperties, topic, KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE),
                topologyProperties.getProperty(CASSANDRA_HOSTS),
                Integer.parseInt(topologyProperties.getProperty(CASSANDRA_PORT)),
                topologyProperties.getProperty(CASSANDRA_KEYSPACE_NAME),
                topologyProperties.getProperty(CASSANDRA_USERNAME),
                topologyProperties.getProperty(CASSANDRA_SECRET_TOKEN));
    }

    private static KafkaSpoutConfig<String, DpsRecord> createKafkaSpoutConfig(String topologyName, Properties topologyProperties, String topic, KafkaSpoutConfig.ProcessingGuarantee processingGuarantee) {
        KafkaSpoutConfig.Builder<String, DpsRecord> configBuilder =
                new KafkaSpoutConfig.Builder<String, DpsRecord>(
                        topologyProperties.getProperty(BOOTSTRAP_SERVERS), topic)
                        .setProcessingGuarantee(processingGuarantee)
                        .setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class)
                        .setProp(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DpsRecordDeserializer.class)
                        .setProp(ConsumerConfig.GROUP_ID_CONFIG, topologyName)
                        .setProp(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, getValue(topologyProperties, MAX_POLL_RECORDS, DEFAULT_MAX_POLL_RECORDS))
                        .setProp(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, getValue(topologyProperties, FETCH_MAX_BYTES, DEFAULT_FETCH_MAX_BYTES))
                        .setFirstPollOffsetStrategy(FirstPollOffsetStrategy.UNCOMMITTED_LATEST)
                        .setOffsetCommitPeriodMs(2000);

        return configBuilder.build();
    }

    public static List<String> addSpouts(TopologyBuilder builder, String topology, Properties topologyProperties) {
        String[] topics = getTopics(topologyProperties);
        List<String> result = new ArrayList<>();
        for (int i = 0; i < topics.length; i++) {
            String spoutName = SPOUT_NAME_PREFIX + (i + 1);
            ECloudSpout eCloudSpout = TopologyHelper.createECloudSpout(topology, topologyProperties, topics[i]);
            builder.setSpout(spoutName, eCloudSpout, 1).setNumTasks(1);
            result.add(spoutName);
        }
        return result;
    }

    public static List<String> addMediaSpouts(TopologyBuilder builder, String topology, Properties topologyProperties) {
        String[] topics = getTopics(topologyProperties);
        List<String> result = new ArrayList<>();
        for (int i = 0; i < topics.length; i++) {
            String spoutName = SPOUT_NAME_PREFIX + (i + 1);
            ECloudSpout eCloudSpout = TopologyHelper.createMediaSpout(topology, topologyProperties, topics[i]);
            builder.setSpout(spoutName, eCloudSpout, 1).setNumTasks(1);
            result.add(spoutName);
        }
        return result;
    }

    public static ECloudSpout createMediaSpout(String topologyName, Properties topologyProperties, String topic) {
        return new MediaSpout(
                topologyName, topic,
                createKafkaSpoutConfig(topologyName, topologyProperties, topic, KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE),
                topologyProperties.getProperty(CASSANDRA_HOSTS),
                Integer.parseInt(topologyProperties.getProperty(CASSANDRA_PORT)),
                topologyProperties.getProperty(CASSANDRA_KEYSPACE_NAME),
                topologyProperties.getProperty(CASSANDRA_USERNAME),
                topologyProperties.getProperty(CASSANDRA_SECRET_TOKEN),
                topologyProperties.getProperty(DEFAULT_MAXIMUM_PARALLELIZATION));
    }


    public static void addSpoutShuffleGrouping(List<String> spoutNames, BoltDeclarer boltDeclarer) {
        for (String spout : spoutNames) {
           // boltDeclarer.shuffleGrouping(spout);
            boltDeclarer.customGrouping(spout, new ShuffleGrouping());
        }
    }

    public static void addSpoutsGroupingToNotificationBolt(List<String> spoutNames, BoltDeclarer boltDeclarer) {
        addSpoutFieldGrouping(spoutNames, boltDeclarer, NOTIFICATION_STREAM_NAME, NotificationTuple.TASK_ID_FIELD_NAME);
    }

    private static void addSpoutFieldGrouping(List<String> spoutNames, BoltDeclarer boltDeclarer,
                                              String streamName, String fieldName) {
        for (String spout : spoutNames) {
            boltDeclarer.fieldsGrouping(spout, streamName, new Fields(fieldName));
        }
    }

    public static void addSpoutFieldGrouping(List<String> spoutNames, BoltDeclarer boltDeclarer,
                                              String fieldName) {
        for (String spout : spoutNames) {
            boltDeclarer.fieldsGrouping(spout, new Fields(fieldName));
         //   boltDeclarer.customGrouping(spout,new GrouperFactory.FieldsGrouper());
        }
    }

    public static void addSpoutThrottlingGrouping(List<String> spoutNames, BoltDeclarer boltDeclarer,
                                             String fieldName) {
        for (String spout : spoutNames) {
            boltDeclarer.customGrouping(spout, new ThrottlingShuffleGrouping());
        }
    }

    private static String[] getTopics(Properties topologyProperties) {
        return topologyProperties.getProperty(TOPICS).split(",");
    }
}
