package eu.europeana.cloud.service.dps.storm.topologies.oaipmh;

import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.io.HarvestingWriteRecordBolt;
import eu.europeana.cloud.service.dps.storm.io.RevisionWriterBolt;
import eu.europeana.cloud.service.dps.storm.io.RevisionWriterBoltForOAI;
import eu.europeana.cloud.service.dps.storm.io.WriteRecordBolt;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.DuplicatedRecordsProcessorBolt;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.OaiHarvestedRecordCategorizationBolt;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.RecordHarvestingBolt;
import eu.europeana.cloud.service.dps.storm.topologies.properties.PropertyFileLoader;
import eu.europeana.cloud.service.dps.storm.utils.DbConnectionDetails;
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

import java.util.List;
import java.util.Properties;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.*;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.*;
import static java.lang.Integer.parseInt;

/**
 *
 */
public class OAIPHMHarvestingTopology {
    private static final Properties topologyProperties = new Properties();
    private static final String TOPOLOGY_PROPERTIES_FILE = "oai-topology-config.properties";
    private static final Logger LOGGER = LoggerFactory.getLogger(OAIPHMHarvestingTopology.class);

    public OAIPHMHarvestingTopology(String defaultPropertyFile, String providedPropertyFile) {
        PropertyFileLoader.loadPropertyFile(defaultPropertyFile, providedPropertyFile, topologyProperties);
    }

    public final StormTopology buildTopology() {
        TopologyBuilder builder = new TopologyBuilder();

        List<String> spoutNames = TopologyHelper.addSpouts(builder, TopologiesNames.OAI_TOPOLOGY, topologyProperties);

        WriteRecordBolt writeRecordBolt = new HarvestingWriteRecordBolt(
                topologyProperties.getProperty(MCS_URL),
                topologyProperties.getProperty(UIS_URL),
                topologyProperties.getProperty(MCS_USER_NAME),
                topologyProperties.getProperty(MCS_USER_PASSWORD)
        );
        RevisionWriterBolt revisionWriterBolt = new RevisionWriterBoltForOAI(
                topologyProperties.getProperty(MCS_URL),
                topologyProperties.getProperty(MCS_USER_NAME),
                topologyProperties.getProperty(MCS_USER_PASSWORD)
        );

        TopologyHelper.addSpoutShuffleGrouping(spoutNames,
                builder.setBolt(RECORD_HARVESTING_BOLT, new RecordHarvestingBolt(), (getAnInt(RECORD_HARVESTING_BOLT_PARALLEL)))
                        .setNumTasks((getAnInt(RECORD_HARVESTING_BOLT_NUMBER_OF_TASKS))));

        builder.setBolt(RECORD_CATEGORIZATION_BOLT, new OaiHarvestedRecordCategorizationBolt(prepareConnectionDetails()),
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

        builder.setBolt(DUPLICATES_DETECTOR_BOLT, new DuplicatedRecordsProcessorBolt(
                                topologyProperties.getProperty(MCS_URL),
                                topologyProperties.getProperty(MCS_USER_NAME),
                                topologyProperties.getProperty(MCS_USER_PASSWORD)
                        ),
                        (getAnInt(DUPLICATES_BOLT_PARALLEL)))
                .setNumTasks((getAnInt(DUPLICATES_BOLT_NUMBER_OF_TASKS)))
                .fieldsGrouping(REVISION_WRITER_BOLT, new Fields(NotificationTuple.TASK_ID_FIELD_NAME));

        TopologyHelper.addSpoutsGroupingToNotificationBolt(
                spoutNames,
                builder.setBolt(NOTIFICATION_BOLT, new NotificationBolt(topologyProperties.getProperty(CASSANDRA_HOSTS),
                                        Integer.parseInt(topologyProperties.getProperty(CASSANDRA_PORT)),
                                        topologyProperties.getProperty(CASSANDRA_KEYSPACE_NAME),
                                        topologyProperties.getProperty(CASSANDRA_USERNAME),
                                        topologyProperties.getProperty(CASSANDRA_SECRET_TOKEN)),
                                getAnInt(NOTIFICATION_BOLT_PARALLEL))
                        .setNumTasks(
                                (getAnInt(NOTIFICATION_BOLT_NUMBER_OF_TASKS)))
                        .fieldsGrouping(RECORD_HARVESTING_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                                new Fields(NotificationTuple.TASK_ID_FIELD_NAME))
                        .fieldsGrouping(RECORD_CATEGORIZATION_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                                new Fields(NotificationTuple.TASK_ID_FIELD_NAME))
                        .fieldsGrouping(WRITE_RECORD_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                                new Fields(NotificationTuple.TASK_ID_FIELD_NAME))
                        .fieldsGrouping(REVISION_WRITER_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                                new Fields(NotificationTuple.TASK_ID_FIELD_NAME))
                        .fieldsGrouping(DUPLICATES_DETECTOR_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                                new Fields(NotificationTuple.TASK_ID_FIELD_NAME)));

        return builder.createTopology();
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
