package eu.europeana.cloud.service.dps.storm.topologies.media.service;

import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.StormTupleKeys;
import eu.europeana.cloud.service.dps.storm.io.AddResultToDataSetBolt;
import eu.europeana.cloud.service.dps.storm.io.ParseFileForMediaBolt;
import eu.europeana.cloud.service.dps.storm.io.RevisionWriterBolt;
import eu.europeana.cloud.service.dps.storm.io.WriteRecordBolt;
import eu.europeana.cloud.service.dps.storm.spout.ECloudSpout;
import eu.europeana.cloud.service.dps.storm.topologies.properties.PropertyFileLoader;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
import eu.europeana.cloud.service.dps.storm.utils.TopologyHelper;
import eu.europeana.cloud.service.dps.storm.utils.TopologySubmitter;
import org.apache.storm.Config;
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

/**
 * Created by Tarek on 12/14/2018.
 */
public class MediaTopology {
    private static Properties topologyProperties = new Properties();
    private static final String TOPOLOGY_PROPERTIES_FILE = "media-topology-config.properties";
    private static final Logger LOGGER = LoggerFactory.getLogger(MediaTopology.class);

    public MediaTopology(String defaultPropertyFile, String providedPropertyFile) {
        PropertyFileLoader.loadPropertyFile(defaultPropertyFile, providedPropertyFile, topologyProperties);
    }

    public final StormTopology buildTopology(String ecloudMcsAddress) {
        TopologyBuilder builder = new TopologyBuilder();

        ECloudSpout eCloudSpout = TopologyHelper.createECloudSpout(TopologiesNames.MEDIA_TOPOLOGY, topologyProperties);

        WriteRecordBolt writeRecordBolt = new WriteRecordBolt(ecloudMcsAddress);
        RevisionWriterBolt revisionWriterBolt = new RevisionWriterBolt(ecloudMcsAddress);
        AmazonClient amazonClient = new AmazonClient(topologyProperties.getProperty(AWS_CREDENTIALS_ACCESSKEY), topologyProperties.getProperty(AWS_CREDENTIALS_SECRETKEY),
                topologyProperties.getProperty(AWS_CREDENTIALS_ENDPOINT), topologyProperties.getProperty(AWS_CREDENTIALS_BUCKET));
        builder.setSpout(SPOUT, eCloudSpout, (getAnInt(KAFKA_SPOUT_PARALLEL)))
                .setNumTasks((getAnInt(KAFKA_SPOUT_NUMBER_OF_TASKS)));

        builder.setBolt(EDM_OBJECT_PROCESSOR_BOLT, new EDMObjectProcessorBolt(ecloudMcsAddress, amazonClient),
                (getAnInt(RESOURCE_PROCESSING_BOLT_PARALLEL)))
                .setNumTasks((getAnInt(RESOURCE_PROCESSING_BOLT_NUMBER_OF_TASKS)))
                .customGrouping(SPOUT, new ShuffleGrouping());

        builder.setBolt(PARSE_FILE_BOLT, new ParseFileForMediaBolt(ecloudMcsAddress),
                (getAnInt(PARSE_FILE_BOLT_PARALLEL)))
                .setNumTasks((getAnInt(PARSE_FILE_BOLT_BOLT_NUMBER_OF_TASKS)))
                .customGrouping(EDM_OBJECT_PROCESSOR_BOLT, new ShuffleGrouping());

        builder.setBolt(RESOURCE_PROCESSING_BOLT, new ResourceProcessingBolt(amazonClient),
                (getAnInt(RESOURCE_PROCESSING_BOLT_PARALLEL)))
                .setNumTasks((getAnInt(RESOURCE_PROCESSING_BOLT_NUMBER_OF_TASKS)))
                .customGrouping(PARSE_FILE_BOLT, new ShuffleGrouping());

        builder.setBolt(EDM_ENRICHMENT_BOLT, new EDMEnrichmentBolt(ecloudMcsAddress),
                (getAnInt(EDM_ENRICHMENT_BOLT_PARALLEL)))
                .setNumTasks((getAnInt(EDM_ENRICHMENT_BOLT_NUMBER_OF_TASKS)))
                .fieldsGrouping(RESOURCE_PROCESSING_BOLT, new Fields(StormTupleKeys.INPUT_FILES_TUPLE_KEY))
                .fieldsGrouping(EDM_OBJECT_PROCESSOR_BOLT, EDMObjectProcessorBolt.EDM_OBJECT_ENRICHMENT_STREAM_NAME,
                        new Fields(StormTupleKeys.INPUT_FILES_TUPLE_KEY));

        builder.setBolt(WRITE_RECORD_BOLT, writeRecordBolt,
                (getAnInt(WRITE_BOLT_PARALLEL)))
                .setNumTasks((getAnInt(WRITE_BOLT_NUMBER_OF_TASKS)))
                .customGrouping(EDM_ENRICHMENT_BOLT, new ShuffleGrouping());

        builder.setBolt(REVISION_WRITER_BOLT, revisionWriterBolt,
                (getAnInt(REVISION_WRITER_BOLT_PARALLEL)))
                .setNumTasks((getAnInt(REVISION_WRITER_BOLT_NUMBER_OF_TASKS)))
                .customGrouping(WRITE_RECORD_BOLT, new ShuffleGrouping());

        AddResultToDataSetBolt addResultToDataSetBolt = new AddResultToDataSetBolt(ecloudMcsAddress);
        builder.setBolt(WRITE_TO_DATA_SET_BOLT, addResultToDataSetBolt,
                (getAnInt(ADD_TO_DATASET_BOLT_PARALLEL)))
                .setNumTasks((getAnInt(ADD_TO_DATASET_BOLT_NUMBER_OF_TASKS)))
                .customGrouping(REVISION_WRITER_BOLT, new ShuffleGrouping());


        builder.setBolt(NOTIFICATION_BOLT, new NotificationBolt(topologyProperties.getProperty(CASSANDRA_HOSTS),
                        Integer.parseInt(topologyProperties.getProperty(CASSANDRA_PORT)),
                        topologyProperties.getProperty(CASSANDRA_KEYSPACE_NAME),
                        topologyProperties.getProperty(CASSANDRA_USERNAME),
                        topologyProperties.getProperty(CASSANDRA_SECRET_TOKEN)),
                getAnInt(NOTIFICATION_BOLT_PARALLEL))
                .setNumTasks(
                        (getAnInt(NOTIFICATION_BOLT_NUMBER_OF_TASKS)))
                .fieldsGrouping(SPOUT, NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.TASK_ID_FIELD_NAME))
                .fieldsGrouping(EDM_OBJECT_PROCESSOR_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.TASK_ID_FIELD_NAME))
                .fieldsGrouping(PARSE_FILE_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.TASK_ID_FIELD_NAME))
                .fieldsGrouping(RESOURCE_PROCESSING_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.TASK_ID_FIELD_NAME))
                .fieldsGrouping(EDM_ENRICHMENT_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.TASK_ID_FIELD_NAME))
                .fieldsGrouping(WRITE_RECORD_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.TASK_ID_FIELD_NAME))
                .fieldsGrouping(REVISION_WRITER_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.TASK_ID_FIELD_NAME))
                .fieldsGrouping(WRITE_TO_DATA_SET_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.TASK_ID_FIELD_NAME));

        return builder.createTopology();
    }

    private static int getAnInt(String propertyName) {
        return parseInt(topologyProperties.getProperty(propertyName));
    }

    public static void main(String[] args) {
        try {
            LOGGER.info("Assembling '{}'", TopologiesNames.MEDIA_TOPOLOGY);

            if (args.length <= 1) {
                String providedPropertyFile = (args.length == 1 ? args[0] : "");

                MediaTopology mediaTopology = new MediaTopology(TOPOLOGY_PROPERTIES_FILE, providedPropertyFile);

                String ecloudMcsAddress = topologyProperties.getProperty(MCS_URL);
                StormTopology stormTopology = mediaTopology.buildTopology(ecloudMcsAddress);
                Config config = buildConfig(topologyProperties);
                LOGGER.info("Submitting '{}'...", topologyProperties.getProperty(TOPOLOGY_NAME));
                TopologySubmitter.submitTopology(topologyProperties.getProperty(TOPOLOGY_NAME), config, stormTopology);
            } else {
                LOGGER.error("Invalid number of parameters");
            }
        } catch (Exception e) {
            LOGGER.error("General error while setting up topology", e);
        }
    }
}
