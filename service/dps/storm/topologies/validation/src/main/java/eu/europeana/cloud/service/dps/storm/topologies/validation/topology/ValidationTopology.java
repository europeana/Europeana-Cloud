package eu.europeana.cloud.service.dps.storm.topologies.validation.topology;

import eu.europeana.cloud.service.dps.InputDataType;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.ParseTaskBolt;
import eu.europeana.cloud.service.dps.storm.io.*;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.CustomKafkaSpout;
import eu.europeana.cloud.service.dps.storm.topologies.properties.PropertyFileLoader;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.*;

import eu.europeana.cloud.service.dps.storm.topologies.validation.topology.bolts.StatisticsBolt;
import eu.europeana.cloud.service.dps.storm.topologies.validation.topology.bolts.ValidationBolt;
import com.google.common.base.Throwables;
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

import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.*;
import static java.lang.Integer.parseInt;


/**
 * Created by Tarek on 12/5/2017.
 */
public class ValidationTopology {
    private static Properties topologyProperties = new Properties();
    private static Properties validationProperties = new Properties();
    private final BrokerHosts brokerHosts;
    private static final String TOPOLOGY_PROPERTIES_FILE = "validation-topology-config.properties";
    private static final String VALIDATION_PROPERTIES_FILE = "validation.properties";
    private final String DATASET_STREAM = InputDataType.DATASET_URLS.name();
    private final String FILE_STREAM = InputDataType.FILE_URLS.name();
    private static final Logger LOGGER = LoggerFactory.getLogger(ValidationTopology.class);
    public static final String SUCCESS_MESSAGE = "The record is validated correctly";

    public ValidationTopology(String defaultPropertyFile, String providedPropertyFile, String defaultValidationPropertiesFile, String providedValidationPropertiesFile) {
        PropertyFileLoader.loadPropertyFile(defaultPropertyFile, providedPropertyFile, topologyProperties);
        PropertyFileLoader.loadPropertyFile(defaultValidationPropertiesFile, providedValidationPropertiesFile, validationProperties);
        brokerHosts = new ZkHosts(topologyProperties.getProperty(INPUT_ZOOKEEPER_ADDRESS));
    }


    public final StormTopology buildTopology(String validationTopic, String ecloudMcsAddress) {
        Map<String, String> routingRules = new HashMap<>();
        routingRules.put(DATASET_STREAM, DATASET_STREAM);
        routingRules.put(FILE_STREAM, FILE_STREAM);
        ReadFileBolt retrieveFileBolt = new ReadFileBolt(ecloudMcsAddress);

        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, validationTopic, "", "storm");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaConfig.ignoreZkOffsets = true;
        kafkaConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        TopologyBuilder builder = new TopologyBuilder();
        KafkaSpout kafkaSpout = new CustomKafkaSpout(kafkaConfig, topologyProperties.getProperty(CASSANDRA_HOSTS),
                Integer.parseInt(topologyProperties.getProperty(CASSANDRA_PORT)),
                topologyProperties.getProperty(CASSANDRA_KEYSPACE_NAME),
                topologyProperties.getProperty(CASSANDRA_USERNAME),
                topologyProperties.getProperty(CASSANDRA_SECRET_TOKEN));

        ValidationRevisionWriter validationRevisionWriter = new ValidationRevisionWriter(ecloudMcsAddress, SUCCESS_MESSAGE);


        builder.setSpout(SPOUT, kafkaSpout,
                (getAnInt(KAFKA_SPOUT_PARALLEL)))
                .setNumTasks(
                        (getAnInt(KAFKA_SPOUT_NUMBER_OF_TASKS)));

        builder.setBolt(PARSE_TASK_BOLT, new ParseTaskBolt(routingRules),
                (getAnInt(PARSE_TASKS_BOLT_PARALLEL)))
                .setNumTasks(
                        (getAnInt(PARSE_TASKS_BOLT_NUMBER_OF_TASKS)))
                .shuffleGrouping(SPOUT);


        builder.setBolt(READ_DATASETS_BOLT, new ReadDatasetsBolt(),
                (getAnInt(READ_DATASETS_BOLT_PARALLEL)))
                .setNumTasks(
                        (getAnInt(READ_DATASETS_BOLT_NUMBER_OF_TASKS)))
                .shuffleGrouping(PARSE_TASK_BOLT, DATASET_STREAM);

        builder.setBolt(READ_DATASET_BOLT, new ReadDatasetBolt(ecloudMcsAddress),
                (getAnInt(READ_DATASET_BOLT_PARALLEL)))
                .setNumTasks(
                        (getAnInt(READ_DATASET_BOLT_NUMBER_OF_TASKS)))
                .shuffleGrouping(READ_DATASETS_BOLT);


        builder.setBolt(READ_REPRESENTATION_BOLT, new ReadRepresentationBolt(ecloudMcsAddress),
                (getAnInt(READ_REPRESENTATION_BOLT_PARALLEL)))
                .setNumTasks(
                        (getAnInt(READ_REPRESENTATION_BOLT_NUMBER_OF_TASKS)))
                .shuffleGrouping(READ_DATASET_BOLT);


        builder.setBolt(RETRIEVE_FILE_BOLT, retrieveFileBolt,
                (getAnInt(RETRIEVE_FILE_BOLT_PARALLEL)))
                .setNumTasks(
                        (getAnInt(RETRIEVE_FILE_BOLT_NUMBER_OF_TASKS)))
                .shuffleGrouping(PARSE_TASK_BOLT, FILE_STREAM).shuffleGrouping(READ_REPRESENTATION_BOLT);

        builder.setBolt(VALIDATION_BOLT, new ValidationBolt(validationProperties),
                (getAnInt(VALIDATION_BOLT_PARALLEL)))
                .setNumTasks((getAnInt(VALIDATION_BOLT_NUMBER_OF_TASKS)))
                .shuffleGrouping(RETRIEVE_FILE_BOLT);

        builder.setBolt(STATISTICS_BOLT, new StatisticsBolt(topologyProperties.getProperty(CASSANDRA_HOSTS),
                        Integer.parseInt(topologyProperties.getProperty(CASSANDRA_PORT)),
                        topologyProperties.getProperty(CASSANDRA_KEYSPACE_NAME),
                        topologyProperties.getProperty(CASSANDRA_USERNAME),
                        topologyProperties.getProperty(CASSANDRA_SECRET_TOKEN)),
                (getAnInt(STATISTICS_BOLT_PARALLEL)))
                .setNumTasks((getAnInt(STATISTICS_BOLT_NUMBER_OF_TASKS)))
                .shuffleGrouping(VALIDATION_BOLT);

        builder.setBolt(REVISION_WRITER_BOLT, validationRevisionWriter,
                (getAnInt(REVISION_WRITER_BOLT_PARALLEL)))
                .setNumTasks(
                        (getAnInt(REVISION_WRITER_BOLT_NUMBER_OF_TASKS)))
                .shuffleGrouping(STATISTICS_BOLT);


        builder.setBolt(NOTIFICATION_BOLT, new NotificationBolt(topologyProperties.getProperty(CASSANDRA_HOSTS),
                        Integer.parseInt(topologyProperties.getProperty(CASSANDRA_PORT)),
                        topologyProperties.getProperty(CASSANDRA_KEYSPACE_NAME),
                        topologyProperties.getProperty(CASSANDRA_USERNAME),
                        topologyProperties.getProperty(CASSANDRA_SECRET_TOKEN)),
                getAnInt(NOTIFICATION_BOLT_PARALLEL))
                .setNumTasks(
                        (getAnInt(NOTIFICATION_BOLT_NUMBER_OF_TASKS)))
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
                .fieldsGrouping(VALIDATION_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(STATISTICS_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(REVISION_WRITER_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName));
        return builder.createTopology();
    }

    private static int getAnInt(String parseTasksBoltParallel) {
        return parseInt(topologyProperties.getProperty(parseTasksBoltParallel));
    }

    public static void main(String[] args) {
        try {
            if (args.length <= 2) {

                String providedValidationPropertiesFile = "";
                String providedPropertyFile = "";
                if (args.length == 1)
                    providedPropertyFile = args[0];
                else if (args.length == 2) {
                    providedPropertyFile = args[0];
                    providedValidationPropertiesFile = args[1];
                }
                ValidationTopology validationTopology = new ValidationTopology(TOPOLOGY_PROPERTIES_FILE, providedPropertyFile, VALIDATION_PROPERTIES_FILE, providedValidationPropertiesFile);
                String topologyName = topologyProperties.getProperty(TOPOLOGY_NAME);
                // kafka topic == topology name
                String kafkaTopic = topologyName;
                String ecloudMcsAddress = topologyProperties.getProperty(MCS_URL);
                StormTopology stormTopology = validationTopology.buildTopology(kafkaTopic, ecloudMcsAddress);
                StormSubmitter.submitTopology(topologyName, configureTopology(topologyProperties), stormTopology);
            }
        } catch (Exception e) {
            LOGGER.error(Throwables.getStackTraceAsString(e));
        }
    }
}

