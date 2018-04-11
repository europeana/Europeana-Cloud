package eu.europeana.cloud.enrichment;

import eu.europeana.cloud.enrichment.bolts.EnrichmentBolt;
import eu.europeana.cloud.service.dps.InputDataType;
import eu.europeana.cloud.service.dps.storm.NotificationBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.ParseTaskBolt;
import eu.europeana.cloud.service.dps.storm.io.*;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.CustomKafkaSpout;
import eu.europeana.cloud.service.dps.storm.topologies.properties.PropertyFileLoader;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Throwables;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static eu.europeana.cloud.service.dps.storm.AbstractDpsBolt.NOTIFICATION_STREAM_NAME;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.*;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.*;

/**
 * Created by Tarek on 1/24/2018.
 */
public class EnrichmentTopology {
    private static Properties topologyProperties;
    private final BrokerHosts brokerHosts;
    private final static String TOPOLOGY_PROPERTIES_FILE = "enrichment-topology-config.properties";
    private final String DATASET_STREAM = InputDataType.DATASET_URLS.name();
    private final String FILE_STREAM = InputDataType.FILE_URLS.name();
    private static final Logger LOGGER = LoggerFactory.getLogger(EnrichmentTopology.class);


    public EnrichmentTopology(String defaultPropertyFile, String providedPropertyFile) {
        topologyProperties = new Properties();
        PropertyFileLoader.loadPropertyFile(defaultPropertyFile, providedPropertyFile, topologyProperties);
        brokerHosts = new ZkHosts(topologyProperties.getProperty(INPUT_ZOOKEEPER_ADDRESS));
    }

    public StormTopology buildTopology(String enrichmentTopic, String ecloudMcsAddress) {


        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, enrichmentTopic, "", "storm");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaConfig.ignoreZkOffsets = true;
        kafkaConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        CustomKafkaSpout kafkaSpout = new CustomKafkaSpout(kafkaConfig, topologyProperties.getProperty(CASSANDRA_HOSTS),
                Integer.parseInt(topologyProperties.getProperty(CASSANDRA_PORT)),
                topologyProperties.getProperty(CASSANDRA_KEYSPACE_NAME),
                topologyProperties.getProperty(CASSANDRA_USERNAME),
                topologyProperties.getProperty(CASSANDRA_SECRET_TOKEN));

        TopologyBuilder builder = new TopologyBuilder();

        Map<String, String> routingRules = new HashMap<>();
        routingRules.put(DATASET_STREAM, DATASET_STREAM);
        routingRules.put(FILE_STREAM, FILE_STREAM);

        ReadFileBolt retrieveFileBolt = new ReadFileBolt(ecloudMcsAddress);
        WriteRecordBolt writeRecordBolt = new WriteRecordBolt(ecloudMcsAddress);
        RevisionWriterBolt revisionWriterBolt = new RevisionWriterBolt(ecloudMcsAddress);
        EnrichmentBolt enrichmentBolt = new EnrichmentBolt(topologyProperties.getProperty(DEREFERENCE_SERVICE_URL), topologyProperties.getProperty(ENRICHMENT_SERVICE_URL));


        // TOPOLOGY STRUCTURE!
        builder.setSpout(SPOUT, kafkaSpout,
                (Integer.parseInt(topologyProperties.getProperty(KAFKA_SPOUT_PARALLEL))))
                .setNumTasks(
                        (Integer.parseInt(topologyProperties.getProperty(KAFKA_SPOUT_NUMBER_OF_TASKS))));

        builder.setBolt(PARSE_TASK_BOLT, new ParseTaskBolt(routingRules),
                (Integer
                        .parseInt(topologyProperties.getProperty(PARSE_TASKS_BOLT_PARALLEL))))
                .setNumTasks(
                        (Integer.parseInt(topologyProperties.getProperty(PARSE_TASKS_BOLT_NUMBER_OF_TASKS))))
                .shuffleGrouping(SPOUT);


        builder.setBolt(READ_DATASETS_BOLT, new ReadDatasetsBolt(),
                (Integer
                        .parseInt(topologyProperties.getProperty(READ_DATASETS_BOLT_PARALLEL))))
                .setNumTasks(
                        (Integer.parseInt(topologyProperties.getProperty(READ_DATASETS_BOLT_NUMBER_OF_TASKS))))
                .shuffleGrouping(PARSE_TASK_BOLT, DATASET_STREAM);

        builder.setBolt(READ_DATASET_BOLT, new ReadDatasetBolt(ecloudMcsAddress),
                (Integer
                        .parseInt(topologyProperties.getProperty(READ_DATASET_BOLT_PARALLEL))))
                .setNumTasks(
                        (Integer.parseInt(topologyProperties.getProperty(READ_DATASET_BOLT_NUMBER_OF_TASKS))))
                .shuffleGrouping(READ_DATASETS_BOLT);


        builder.setBolt(READ_REPRESENTATION_BOLT, new ReadRepresentationBolt(ecloudMcsAddress),
                (Integer
                        .parseInt(topologyProperties.getProperty(READ_REPRESENTATION_BOLT_PARALLEL))))
                .setNumTasks(
                        (Integer.parseInt(topologyProperties.getProperty(READ_REPRESENTATION_BOLT_NUMBER_OF_TASKS))))
                .shuffleGrouping(READ_DATASET_BOLT);


        builder.setBolt(RETRIEVE_FILE_BOLT, retrieveFileBolt,
                (Integer
                        .parseInt(topologyProperties.getProperty(RETRIEVE_FILE_BOLT_PARALLEL))))
                .setNumTasks(
                        (Integer.parseInt(topologyProperties.getProperty(RETRIEVE_FILE_BOLT_NUMBER_OF_TASKS))))
                .shuffleGrouping(PARSE_TASK_BOLT, FILE_STREAM).shuffleGrouping(READ_REPRESENTATION_BOLT);


        builder.setBolt(ENRICHMENT_BOLT, enrichmentBolt,
                (Integer.parseInt(topologyProperties.getProperty(ENRICHMENT_BOLT_PARALLEL))))
                .setNumTasks(
                        (Integer.parseInt(topologyProperties.getProperty(ENRICHMENT_BOLT_NUMBER_OF_TASKS))))
                .shuffleGrouping(RETRIEVE_FILE_BOLT);

        builder.setBolt(WRITE_RECORD_BOLT, writeRecordBolt,
                (Integer.parseInt(topologyProperties.getProperty(WRITE_BOLT_PARALLEL))))
                .setNumTasks(
                        (Integer.parseInt(topologyProperties.getProperty(WRITE_BOLT_NUMBER_OF_TASKS))))
                .shuffleGrouping(ENRICHMENT_BOLT);


        builder.setBolt(REVISION_WRITER_BOLT, revisionWriterBolt,
                (Integer.parseInt(topologyProperties.getProperty(REVISION_WRITER_BOLT_PARALLEL))))
                .setNumTasks(
                        (Integer.parseInt(topologyProperties.getProperty(Revision_WRITER_BOLT_NUMBER_OF_TASKS))))
                .shuffleGrouping(WRITE_RECORD_BOLT);

        AddResultToDataSetBolt addResultToDataSetBolt = new AddResultToDataSetBolt(ecloudMcsAddress);
        builder.setBolt(WRITE_TO_DATA_SET_BOLT, addResultToDataSetBolt,
                (Integer.parseInt(topologyProperties.getProperty(ADD_TO_DATASET_BOLT_PARALLEL))))
                .setNumTasks(
                        (Integer.parseInt(topologyProperties.getProperty(ADD_TO_DATASET_BOLT_NUMBER_OF_TASKS))))
                .shuffleGrouping(REVISION_WRITER_BOLT);


        builder.setBolt(NOTIFICATION_BOLT, new NotificationBolt(topologyProperties.getProperty(CASSANDRA_HOSTS),
                        Integer.parseInt(topologyProperties.getProperty(CASSANDRA_PORT)),
                        topologyProperties.getProperty(CASSANDRA_KEYSPACE_NAME),
                        topologyProperties.getProperty(CASSANDRA_USERNAME),
                        topologyProperties.getProperty(CASSANDRA_SECRET_TOKEN)),
                Integer.parseInt(topologyProperties.getProperty(NOTIFICATION_BOLT_PARALLEL)))
                .setNumTasks(
                        (Integer.parseInt(topologyProperties.getProperty(NOTIFICATION_BOLT_NUMBER_OF_TASKS))))
                .fieldsGrouping(PARSE_TASK_BOLT, NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(RETRIEVE_FILE_BOLT, NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(READ_DATASETS_BOLT, NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(READ_DATASET_BOLT, NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(READ_REPRESENTATION_BOLT, NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(ENRICHMENT_BOLT, NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(WRITE_RECORD_BOLT, NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(REVISION_WRITER_BOLT, NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(WRITE_TO_DATA_SET_BOLT, NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName));


        return builder.createTopology();
    }

    public static void main(String[] args) {
        try {
            if (args.length <= 1) {

                String providedPropertyFile = "";
                if (args.length == 1) {
                    providedPropertyFile = args[0];
                }

                EnrichmentTopology enrichmentTopology = new EnrichmentTopology(TOPOLOGY_PROPERTIES_FILE, providedPropertyFile);
                String topologyName = topologyProperties.getProperty(TOPOLOGY_NAME);

                // assuming kafka topic == topology name
                String kafkaTopic = topologyName;

                String ecloudMcsAddress = topologyProperties.getProperty(MCS_URL);

                StormTopology stormTopology = enrichmentTopology.buildTopology(
                        kafkaTopic,
                        ecloudMcsAddress);

                Config config = prepareConfig(topologyProperties);
                StormSubmitter.submitTopology(topologyName, config, stormTopology);
            }
        } catch (Exception e) {
            LOGGER.error(Throwables.getStackTraceAsString(e));
        }
    }


}

