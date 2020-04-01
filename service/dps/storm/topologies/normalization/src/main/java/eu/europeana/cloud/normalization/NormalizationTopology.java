package eu.europeana.cloud.normalization;

import eu.europeana.cloud.normalization.bolts.NormalizationBolt;
import eu.europeana.cloud.service.dps.storm.NotificationBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.io.AddResultToDataSetBolt;
import eu.europeana.cloud.service.dps.storm.io.ReadFileBolt;
import eu.europeana.cloud.service.dps.storm.io.RevisionWriterBolt;
import eu.europeana.cloud.service.dps.storm.io.WriteRecordBolt;
import eu.europeana.cloud.service.dps.storm.spout.ECloudSpout;
import eu.europeana.cloud.service.dps.storm.topologies.properties.PropertyFileLoader;
import eu.europeana.cloud.service.dps.storm.utils.TopologyHelper;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.grouping.ShuffleGrouping;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static eu.europeana.cloud.service.dps.storm.AbstractDpsBolt.NOTIFICATION_STREAM_NAME;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.*;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.*;
import static java.lang.Integer.parseInt;

public class NormalizationTopology {

    private static final Logger LOGGER = LoggerFactory.getLogger(NormalizationTopology.class);

    private static final String TOPOLOGY_PROPERTIES_FILE = "normalization-topology-config.properties";
    private static Properties topologyProperties;

    public NormalizationTopology(String defaultPropertyFile, String providedPropertyFile) {
        topologyProperties = new Properties();
        PropertyFileLoader.loadPropertyFile(defaultPropertyFile, providedPropertyFile, topologyProperties);
    }

    public StormTopology buildTopology(String ecloudMcsAddress) {
        TopologyBuilder builder = new TopologyBuilder();

        ECloudSpout eCloudSpout = TopologyHelper.createECloudSpout(topologyProperties);

        ReadFileBolt retrieveFileBolt = new ReadFileBolt(ecloudMcsAddress);
        WriteRecordBolt writeRecordBolt = new WriteRecordBolt(ecloudMcsAddress);
        RevisionWriterBolt revisionWriterBolt = new RevisionWriterBolt(ecloudMcsAddress);
        NormalizationBolt normalizationBolt = new NormalizationBolt();

        // TOPOLOGY STRUCTURE!
        builder.setSpout(SPOUT, eCloudSpout,
                getAnInt(KAFKA_SPOUT_PARALLEL))
                .setNumTasks(
                        (getAnInt(KAFKA_SPOUT_NUMBER_OF_TASKS)));

        builder.setBolt(RETRIEVE_FILE_BOLT, retrieveFileBolt,
                getAnInt(RETRIEVE_FILE_BOLT_PARALLEL))
                .setNumTasks(
                        getAnInt(RETRIEVE_FILE_BOLT_NUMBER_OF_TASKS))
                .customGrouping(SPOUT, new ShuffleGrouping());

        builder.setBolt(NORMALIZATION_BOLT, normalizationBolt,
                getAnInt(NORMALIZATION_BOLT_PARALLEL))
                .setNumTasks(
                        getAnInt(NORMALIZATION_BOLT_NUMBER_OF_TASKS))
                .customGrouping(RETRIEVE_FILE_BOLT, new ShuffleGrouping());

        builder.setBolt(WRITE_RECORD_BOLT, writeRecordBolt,
                getAnInt(WRITE_BOLT_PARALLEL))
                .setNumTasks(
                        getAnInt(WRITE_BOLT_NUMBER_OF_TASKS))
                .customGrouping(NORMALIZATION_BOLT, new ShuffleGrouping());


        builder.setBolt(REVISION_WRITER_BOLT, revisionWriterBolt,
                getAnInt(REVISION_WRITER_BOLT_PARALLEL))
                .setNumTasks(
                        getAnInt(REVISION_WRITER_BOLT_NUMBER_OF_TASKS))
                .customGrouping(WRITE_RECORD_BOLT, new ShuffleGrouping());

        AddResultToDataSetBolt addResultToDataSetBolt = new AddResultToDataSetBolt(ecloudMcsAddress);
        builder.setBolt(WRITE_TO_DATA_SET_BOLT, addResultToDataSetBolt,
                getAnInt(ADD_TO_DATASET_BOLT_PARALLEL))
                .setNumTasks(
                        getAnInt(ADD_TO_DATASET_BOLT_NUMBER_OF_TASKS))
                .shuffleGrouping(REVISION_WRITER_BOLT);


        builder.setBolt(NOTIFICATION_BOLT, new NotificationBolt(topologyProperties.getProperty(CASSANDRA_HOSTS),
                        getAnInt(CASSANDRA_PORT),
                        topologyProperties.getProperty(CASSANDRA_KEYSPACE_NAME),
                        topologyProperties.getProperty(CASSANDRA_USERNAME),
                        topologyProperties.getProperty(CASSANDRA_SECRET_TOKEN)),
                getAnInt(NOTIFICATION_BOLT_PARALLEL))
                .setNumTasks(
                        getAnInt(NOTIFICATION_BOLT_NUMBER_OF_TASKS))
                .fieldsGrouping(SPOUT, NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(RETRIEVE_FILE_BOLT, NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(NORMALIZATION_BOLT, NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(WRITE_RECORD_BOLT, NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(REVISION_WRITER_BOLT, NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(WRITE_TO_DATA_SET_BOLT, NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName));


        return builder.createTopology();
    }

    public static void main(String... args) {
        try {
            LOGGER.info("Assembling '{}'", topologyProperties.getProperty(TOPOLOGY_NAME));
            if (args.length <= 1) {
                String providedPropertyFile = (args.length == 1 ? args[0] : "");

                NormalizationTopology normalizationTopology =
                        new NormalizationTopology(TOPOLOGY_PROPERTIES_FILE, providedPropertyFile);

                String ecloudMcsAddress = topologyProperties.getProperty(MCS_URL);
                StormTopology stormTopology = normalizationTopology.buildTopology(ecloudMcsAddress);
                Config config = configureTopology(topologyProperties);
                LOGGER.info("Submitting '{}'...", topologyProperties.getProperty(TOPOLOGY_NAME));
                StormSubmitter.submitTopology(topologyProperties.getProperty(TOPOLOGY_NAME), config, stormTopology);
            } else {
                LOGGER.error("Invalid number of parameters");
            }
        } catch (Exception e) {
            LOGGER.error("General error while setting up topology", e);
        }
    }

    private static int getAnInt(String propertyName) {
        return parseInt(topologyProperties.getProperty(propertyName));
    }
}
