package eu.europeana.cloud.service.dps.storm.topologies.indexing;

import com.google.common.base.Throwables;
import eu.europeana.cloud.service.dps.InputDataType;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.ParseTaskBolt;
import eu.europeana.cloud.service.dps.storm.io.*;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.CustomKafkaSpout;
import eu.europeana.cloud.service.dps.storm.topologies.indexing.bolts.IndexingBolt;
import eu.europeana.cloud.service.dps.storm.topologies.properties.PropertyFileLoader;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.*;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.*;

/**
 * Created by pwozniak on 4/6/18
 */
public class IndexingTopology {

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexingTopology.class);

    private static Properties topologyProperties = new Properties();
    private static Properties indexingProperties = new Properties();
    private final BrokerHosts brokerHosts;
    private static final String TOPOLOGY_PROPERTIES_FILE = "indexing-topology-config.properties";
    private static final String INDEXING_PROPERTIES_FILE = "indexing.properties";
    private final String DATASET_STREAM = InputDataType.DATASET_URLS.name();
    private final String FILE_STREAM = InputDataType.FILE_URLS.name();
    public static final String SUCCESS_MESSAGE = "Record is indexed correctly";

    private IndexingTopology(String defaultPropertyFile, String providedPropertyFile, String defaultIndexingPropertiesFile, String providedIndexingPropertiesFile) {
        PropertyFileLoader.loadPropertyFile(defaultPropertyFile, providedPropertyFile, topologyProperties);
        PropertyFileLoader.loadPropertyFile(defaultIndexingPropertiesFile, providedIndexingPropertiesFile, indexingProperties);
        brokerHosts = new ZkHosts(topologyProperties.getProperty(INPUT_ZOOKEEPER_ADDRESS));
    }

    private StormTopology buildTopology(String indexingTopic, String ecloudMcsAddress) {
        Map<String, String> routingRules = new HashMap<>();
        routingRules.put(DATASET_STREAM, DATASET_STREAM);
        routingRules.put(FILE_STREAM, FILE_STREAM);
        ReadFileBolt retrieveFileBolt = new ReadFileBolt(ecloudMcsAddress);

        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, indexingTopic, "", "storm");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaConfig.ignoreZkOffsets = true;
        kafkaConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        TopologyBuilder builder = new TopologyBuilder();
        KafkaSpout kafkaSpout = new CustomKafkaSpout(kafkaConfig, topologyProperties.getProperty(CASSANDRA_HOSTS),
                getAnInt(CASSANDRA_PORT),
                topologyProperties.getProperty(CASSANDRA_KEYSPACE_NAME),
                topologyProperties.getProperty(CASSANDRA_USERNAME),
                topologyProperties.getProperty(CASSANDRA_SECRET_TOKEN));


        builder.setSpout(SPOUT, kafkaSpout,
                getAnInt(KAFKA_SPOUT_PARALLEL))
                .setNumTasks(getAnInt(KAFKA_SPOUT_NUMBER_OF_TASKS));

        builder.setBolt(PARSE_TASK_BOLT, new ParseTaskBolt(routingRules),
                getAnInt(PARSE_TASKS_BOLT_PARALLEL))
                .setNumTasks(getAnInt(PARSE_TASKS_BOLT_NUMBER_OF_TASKS))
                .shuffleGrouping(SPOUT);

        builder.setBolt(READ_DATASETS_BOLT, new ReadDatasetsBolt(),
                getAnInt(READ_DATASETS_BOLT_PARALLEL))
                .setNumTasks(getAnInt(READ_DATASETS_BOLT_NUMBER_OF_TASKS))
                .shuffleGrouping(PARSE_TASK_BOLT, DATASET_STREAM);

        builder.setBolt(READ_DATASET_BOLT, new ReadDatasetBolt(ecloudMcsAddress),
                getAnInt(READ_DATASET_BOLT_PARALLEL))
                .setNumTasks(getAnInt(READ_DATASET_BOLT_NUMBER_OF_TASKS))
                .shuffleGrouping(READ_DATASETS_BOLT);


        builder.setBolt(READ_REPRESENTATION_BOLT, new ReadRepresentationBolt(ecloudMcsAddress),
                getAnInt(READ_REPRESENTATION_BOLT_PARALLEL))
                .setNumTasks(getAnInt(READ_REPRESENTATION_BOLT_NUMBER_OF_TASKS))
                .shuffleGrouping(READ_DATASET_BOLT);

        builder.setBolt(RETRIEVE_FILE_BOLT, retrieveFileBolt,
                getAnInt(RETRIEVE_FILE_BOLT_PARALLEL))
                .setNumTasks(getAnInt(RETRIEVE_FILE_BOLT_NUMBER_OF_TASKS))
                .shuffleGrouping(PARSE_TASK_BOLT, FILE_STREAM).shuffleGrouping(READ_REPRESENTATION_BOLT);

        builder.setBolt(INDEXING_BOLT, new IndexingBolt(indexingProperties),
                getAnInt(INDEXING_BOLT_PARALLEL))
                .setNumTasks(getAnInt(INDEXING_BOLT_NUMBER_OF_TASKS))
                .shuffleGrouping(RETRIEVE_FILE_BOLT);

        builder.setBolt(REVISION_WRITER_BOLT, new ValidationRevisionWriter(ecloudMcsAddress, SUCCESS_MESSAGE),
                getAnInt(REVISION_WRITER_BOLT_PARALLEL))
                .setNumTasks(getAnInt(REVISION_WRITER_BOLT_NUMBER_OF_TASKS))
                .shuffleGrouping(INDEXING_BOLT);

        builder.setBolt(NOTIFICATION_BOLT, new NotificationBolt(topologyProperties.getProperty(CASSANDRA_HOSTS),
                        getAnInt(CASSANDRA_PORT),
                        topologyProperties.getProperty(CASSANDRA_KEYSPACE_NAME),
                        topologyProperties.getProperty(CASSANDRA_USERNAME),
                        topologyProperties.getProperty(CASSANDRA_SECRET_TOKEN)),
                getAnInt(NOTIFICATION_BOLT_PARALLEL))
                .setNumTasks(
                        getAnInt(NOTIFICATION_BOLT_NUMBER_OF_TASKS))
                .fieldsGrouping(PARSE_TASK_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(RETRIEVE_FILE_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(READ_DATASETS_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(READ_DATASET_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(READ_REPRESENTATION_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(INDEXING_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(REVISION_WRITER_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName));
        return builder.createTopology();
    }

    public static void main(String[] args) {
        try {
            LOGGER.info("Assembling indexing topology");

            Config config = new Config();

            if (args.length <= 2) {

                String providedIndexingPropertiesFile = "";
                String providedPropertyFile = "";
                if (args.length == 1)
                    providedPropertyFile = args[0];
                else if (args.length == 2) {
                    providedPropertyFile = args[0];
                    providedIndexingPropertiesFile = args[1];
                }
                IndexingTopology indexingTopology = new IndexingTopology(TOPOLOGY_PROPERTIES_FILE, providedPropertyFile, INDEXING_PROPERTIES_FILE, providedIndexingPropertiesFile);
                String topologyName = topologyProperties.getProperty(TOPOLOGY_NAME);
                // kafka topic == topology name
                String ecloudMcsAddress = topologyProperties.getProperty(MCS_URL);
                StormTopology stormTopology = indexingTopology.buildTopology(topologyName, ecloudMcsAddress);
                config.setNumWorkers(getAnInt(WORKER_COUNT));
                config.setMaxTaskParallelism(getAnInt(MAX_TASK_PARALLELISM));

                LOGGER.info("Submitting indexing topology");
                StormSubmitter.submitTopology(topologyName, config, stormTopology);
            }
        } catch (Exception e) {
            LOGGER.error(Throwables.getStackTraceAsString(e));
        }

    }

    private static int getAnInt(String propertyName) {
        return Integer.parseInt(topologyProperties.getProperty(propertyName));
    }
}
