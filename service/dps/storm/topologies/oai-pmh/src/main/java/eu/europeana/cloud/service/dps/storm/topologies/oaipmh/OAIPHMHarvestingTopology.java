package eu.europeana.cloud.service.dps.storm.topologies.oaipmh;

import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.OAINotificationBolt;
import eu.europeana.cloud.service.dps.storm.io.HarvestingAddToDatasetBolt;
import eu.europeana.cloud.service.dps.storm.io.HarvestingWriteRecordBolt;
import eu.europeana.cloud.service.dps.storm.io.RevisionWriterBolt;
import eu.europeana.cloud.service.dps.storm.io.WriteRecordBolt;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.DuplicatedRecordsProcessorBolt;
import eu.europeana.cloud.service.dps.storm.spout.ECloudSpout;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.HarvestedRecordCategorizationBolt;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.RecordHarvestingBolt;
import eu.europeana.cloud.service.dps.storm.topologies.properties.PropertyFileLoader;
import eu.europeana.cloud.service.dps.storm.utils.DbConnectionDetails;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
import eu.europeana.cloud.service.dps.storm.utils.TopologyHelper;
import eu.europeana.cloud.service.dps.storm.utils.TopologySubmitter;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.grouping.ShuffleGrouping;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
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
 *
 */
public class OAIPHMHarvestingTopology {
    private static Properties topologyProperties=new Properties();
    private static final String TOPOLOGY_PROPERTIES_FILE = "oai-topology-config.properties";
    private static final Logger LOGGER = LoggerFactory.getLogger(OAIPHMHarvestingTopology.class);

    public OAIPHMHarvestingTopology(String defaultPropertyFile, String providedPropertyFile) {
        PropertyFileLoader.loadPropertyFile(defaultPropertyFile, providedPropertyFile, topologyProperties);
    }

    public final StormTopology buildTopology() {
        TopologyBuilder builder = new TopologyBuilder();

        ECloudSpout eCloudSpout = TopologyHelper.createECloudSpout(
                TopologiesNames.OAI_TOPOLOGY, topologyProperties, KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE);

        String mcsServer = topologyProperties.getProperty(MCS_URL);
        String uisServer = topologyProperties.getProperty(UIS_URL);

        WriteRecordBolt writeRecordBolt = new HarvestingWriteRecordBolt(mcsServer, uisServer);
        RevisionWriterBolt revisionWriterBolt = new RevisionWriterBolt(mcsServer);
        HarvestingAddToDatasetBolt addResultToDataSetBolt = new HarvestingAddToDatasetBolt(mcsServer);

        builder.setSpout(SPOUT, eCloudSpout, 1);

        builder.setBolt(RECORD_HARVESTING_BOLT, new RecordHarvestingBolt(),
                (getAnInt(RECORD_HARVESTING_BOLT_PARALLEL)))
                .setNumTasks((getAnInt(RECORD_HARVESTING_BOLT_NUMBER_OF_TASKS)))
                .customGrouping(SPOUT, new ShuffleGrouping());

        builder.setBolt(RECORD_CATEGORIZATION_BOLT, new HarvestedRecordCategorizationBolt(prepareConnectionDetails()),
                (getAnInt(RECORD_HARVESTING_BOLT_PARALLEL)))
                .setNumTasks((getAnInt(RECORD_HARVESTING_BOLT_NUMBER_OF_TASKS)))
                .customGrouping(RECORD_HARVESTING_BOLT, new ShuffleGrouping());

        builder.setBolt(WRITE_RECORD_BOLT, writeRecordBolt,
                (getAnInt(WRITE_BOLT_PARALLEL)))
                .setNumTasks((getAnInt(WRITE_BOLT_NUMBER_OF_TASKS)))
                .customGrouping(RECORD_CATEGORIZATION_BOLT, new ShuffleGrouping());

        builder.setBolt(REVISION_WRITER_BOLT, revisionWriterBolt,
                (getAnInt(REVISION_WRITER_BOLT_PARALLEL)))
                .setNumTasks((getAnInt(REVISION_WRITER_BOLT_NUMBER_OF_TASKS)))
                .customGrouping(WRITE_RECORD_BOLT, new ShuffleGrouping());

        builder.setBolt(DUPLICATES_DETECTOR_BOLT, new DuplicatedRecordsProcessorBolt(mcsServer),
                (getAnInt(DUPLICATES_BOLT_PARALLEL)))
                .setNumTasks((getAnInt(DUPLICATES_BOLT_NUMBER_OF_TASKS)))
                .fieldsGrouping(REVISION_WRITER_BOLT, new Fields(NotificationTuple.taskIdFieldName));

        builder.setBolt(WRITE_TO_DATA_SET_BOLT, addResultToDataSetBolt,
                (getAnInt(ADD_TO_DATASET_BOLT_PARALLEL)))
                .setNumTasks((getAnInt(ADD_TO_DATASET_BOLT_NUMBER_OF_TASKS)))
                .customGrouping(DUPLICATES_DETECTOR_BOLT, new ShuffleGrouping());

        builder.setBolt(NOTIFICATION_BOLT, new OAINotificationBolt(topologyProperties.getProperty(CASSANDRA_HOSTS),
                        Integer.parseInt(topologyProperties.getProperty(CASSANDRA_PORT)),
                        topologyProperties.getProperty(CASSANDRA_KEYSPACE_NAME),
                        topologyProperties.getProperty(CASSANDRA_USERNAME),
                        topologyProperties.getProperty(CASSANDRA_SECRET_TOKEN)),
                getAnInt(NOTIFICATION_BOLT_PARALLEL))
                .setNumTasks(
                        (getAnInt(NOTIFICATION_BOLT_NUMBER_OF_TASKS)))
                .fieldsGrouping(SPOUT, NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(RECORD_HARVESTING_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(RECORD_CATEGORIZATION_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(WRITE_RECORD_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(REVISION_WRITER_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(DUPLICATES_DETECTOR_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(WRITE_TO_DATA_SET_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName));

        return builder.createTopology();
    }

    public static Properties getProperties() {
        return topologyProperties;
    }

    private static int getAnInt(String parseTasksBoltParallel) {
        return parseInt(topologyProperties.getProperty(parseTasksBoltParallel));
    }

    private DbConnectionDetails prepareConnectionDetails() {
        return DbConnectionDetails.builder()
                .hosts(topologyProperties.getProperty(CASSANDRA_HOSTS))
                .port(getAnInt(CASSANDRA_PORT))
                .keyspaceName(topologyProperties.getProperty(CASSANDRA_KEYSPACE_NAME))
                .userName(topologyProperties.getProperty(CASSANDRA_USERNAME))
                .password(topologyProperties.getProperty(CASSANDRA_SECRET_TOKEN))
                .build();
    }

    public static void main(String[] args) {
        try {
            LOGGER.info("Assembling '{}'", TopologiesNames.OAI_TOPOLOGY);
            if (args.length <= 1) {
                String providedPropertyFile = (args.length == 1 ? args[0] : "");

                OAIPHMHarvestingTopology oaiphmHarvestingTopology =
                        new OAIPHMHarvestingTopology(TOPOLOGY_PROPERTIES_FILE, providedPropertyFile);

                StormTopology stormTopology = oaiphmHarvestingTopology.buildTopology();
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
