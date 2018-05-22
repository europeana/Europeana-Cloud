package eu.europeana.cloud.service.dps.storm.topologies.validation.topology;

import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.io.*;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.MCSReaderSpout;
import eu.europeana.cloud.service.dps.storm.topologies.properties.PropertyFileLoader;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.*;

import eu.europeana.cloud.service.dps.storm.topologies.validation.topology.bolts.StatisticsBolt;
import eu.europeana.cloud.service.dps.storm.topologies.validation.topology.bolts.ValidationBolt;
import com.google.common.base.Throwables;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.grouping.ShuffleGrouping;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private static final Logger LOGGER = LoggerFactory.getLogger(ValidationTopology.class);
    public static final String SUCCESS_MESSAGE = "The record is validated correctly";

    public ValidationTopology(String defaultPropertyFile, String providedPropertyFile, String defaultValidationPropertiesFile, String providedValidationPropertiesFile) {
        PropertyFileLoader.loadPropertyFile(defaultPropertyFile, providedPropertyFile, topologyProperties);
        PropertyFileLoader.loadPropertyFile(defaultValidationPropertiesFile, providedValidationPropertiesFile, validationProperties);
        brokerHosts = new ZkHosts(topologyProperties.getProperty(INPUT_ZOOKEEPER_ADDRESS));
    }


    public final StormTopology buildTopology(String validationTopic, String ecloudMcsAddress) {

        ReadFileBolt retrieveFileBolt = new ReadFileBolt(ecloudMcsAddress);

        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, validationTopic, "", "storm");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaConfig.ignoreZkOffsets = true;
        kafkaConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        MCSReaderSpout mcsReaderSpout = new MCSReaderSpout(kafkaConfig, topologyProperties.getProperty(CASSANDRA_HOSTS),
                Integer.parseInt(topologyProperties.getProperty(CASSANDRA_PORT)),
                topologyProperties.getProperty(CASSANDRA_KEYSPACE_NAME),
                topologyProperties.getProperty(CASSANDRA_USERNAME),
                topologyProperties.getProperty(CASSANDRA_SECRET_TOKEN), ecloudMcsAddress);

        ValidationRevisionWriter validationRevisionWriter = new ValidationRevisionWriter(ecloudMcsAddress, SUCCESS_MESSAGE);
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(SPOUT, mcsReaderSpout,
                (getAnInt(KAFKA_SPOUT_PARALLEL)))
                .setNumTasks(
                        (getAnInt(KAFKA_SPOUT_NUMBER_OF_TASKS)));

        builder.setBolt(RETRIEVE_FILE_BOLT, retrieveFileBolt,
                (getAnInt(RETRIEVE_FILE_BOLT_PARALLEL)))
                .setNumTasks(
                        (getAnInt(RETRIEVE_FILE_BOLT_NUMBER_OF_TASKS)))
                .customGrouping(SPOUT,new ShuffleGrouping());

        builder.setBolt(VALIDATION_BOLT, new ValidationBolt(validationProperties),
                (getAnInt(VALIDATION_BOLT_PARALLEL)))
                .setNumTasks((getAnInt(VALIDATION_BOLT_NUMBER_OF_TASKS)))
                .customGrouping(RETRIEVE_FILE_BOLT,new ShuffleGrouping());

        builder.setBolt(STATISTICS_BOLT, new StatisticsBolt(topologyProperties.getProperty(CASSANDRA_HOSTS),
                        Integer.parseInt(topologyProperties.getProperty(CASSANDRA_PORT)),
                        topologyProperties.getProperty(CASSANDRA_KEYSPACE_NAME),
                        topologyProperties.getProperty(CASSANDRA_USERNAME),
                        topologyProperties.getProperty(CASSANDRA_SECRET_TOKEN)),
                (getAnInt(STATISTICS_BOLT_PARALLEL)))
                .setNumTasks((getAnInt(STATISTICS_BOLT_NUMBER_OF_TASKS)))
                .customGrouping(VALIDATION_BOLT,new ShuffleGrouping());

        builder.setBolt(REVISION_WRITER_BOLT, validationRevisionWriter,
                (getAnInt(REVISION_WRITER_BOLT_PARALLEL)))
                .setNumTasks(
                        (getAnInt(REVISION_WRITER_BOLT_NUMBER_OF_TASKS)))
                .customGrouping(STATISTICS_BOLT,new ShuffleGrouping());


        builder.setBolt(NOTIFICATION_BOLT, new NotificationBolt(topologyProperties.getProperty(CASSANDRA_HOSTS),
                        Integer.parseInt(topologyProperties.getProperty(CASSANDRA_PORT)),
                        topologyProperties.getProperty(CASSANDRA_KEYSPACE_NAME),
                        topologyProperties.getProperty(CASSANDRA_USERNAME),
                        topologyProperties.getProperty(CASSANDRA_SECRET_TOKEN)),
                getAnInt(NOTIFICATION_BOLT_PARALLEL))
                .setNumTasks(
                        (getAnInt(NOTIFICATION_BOLT_NUMBER_OF_TASKS)))
                .fieldsGrouping(SPOUT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(RETRIEVE_FILE_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
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
                Config config = configureTopology(topologyProperties);
                config.setNumAckers(0);
                StormSubmitter.submitTopology(topologyName, config, stormTopology);
            }
        } catch (Exception e) {
            LOGGER.error(Throwables.getStackTraceAsString(e));
        }
    }
}

