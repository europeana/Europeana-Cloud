package eu.europeana.cloud.service.dps.storm.topologies.indexing;

import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.IndexingNotificationBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.io.IndexingRevisionWriter;
import eu.europeana.cloud.service.dps.storm.io.ReadFileBolt;
import eu.europeana.cloud.service.dps.storm.spout.ECloudSpout;
import eu.europeana.cloud.service.dps.storm.topologies.indexing.bolts.IndexingBolt;
import eu.europeana.cloud.service.dps.storm.topologies.properties.PropertyFileLoader;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
import eu.europeana.cloud.service.dps.storm.utils.TopologyHelper;
import eu.europeana.cloud.service.dps.storm.utils.TopologyPropertiesValidator;
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

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.*;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.*;

/**
 * Created by pwozniak on 4/6/18
 */
public class IndexingTopology {

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexingTopology.class);

    private static Properties topologyProperties = new Properties();
    private static Properties indexingProperties = new Properties();
    private static final String TOPOLOGY_PROPERTIES_FILE = "indexing-topology-config.properties";
    private static final String INDEXING_PROPERTIES_FILE = "indexing.properties";
    public static final String SUCCESS_MESSAGE = "Record is indexed correctly";

    private IndexingTopology(String defaultPropertyFile, String providedPropertyFile,
                             String defaultIndexingPropertiesFile, String providedIndexingPropertiesFile) {

        PropertyFileLoader.loadPropertyFile(defaultPropertyFile, providedPropertyFile, topologyProperties);
        PropertyFileLoader.loadPropertyFile(defaultIndexingPropertiesFile, providedIndexingPropertiesFile, indexingProperties);
        TopologyPropertiesValidator.validateFor(TopologiesNames.INDEXING_TOPOLOGY, topologyProperties);
    }

    private StormTopology buildTopology() {

        TopologyBuilder builder = new TopologyBuilder();
        String ecloudMcsAddress = topologyProperties.getProperty(MCS_URL);
        ReadFileBolt readFileBolt = new ReadFileBolt(ecloudMcsAddress);

        ECloudSpout eCloudSpout = TopologyHelper.createECloudSpout(
                TopologiesNames.INDEXING_TOPOLOGY,
                topologyProperties,
                KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE);

        builder.setSpout(SPOUT, eCloudSpout,
                getAnInt(KAFKA_SPOUT_PARALLEL))
                .setNumTasks(getAnInt(KAFKA_SPOUT_NUMBER_OF_TASKS));

        builder.setBolt(RETRIEVE_FILE_BOLT, readFileBolt,
                getAnInt(RETRIEVE_FILE_BOLT_PARALLEL))
                .setNumTasks(getAnInt(RETRIEVE_FILE_BOLT_NUMBER_OF_TASKS))
                .customGrouping(SPOUT, new ShuffleGrouping());

        builder.setBolt(INDEXING_BOLT, new IndexingBolt(indexingProperties),
                getAnInt(INDEXING_BOLT_PARALLEL))
                .setNumTasks(getAnInt(INDEXING_BOLT_NUMBER_OF_TASKS))
                .customGrouping(RETRIEVE_FILE_BOLT, new ShuffleGrouping());

        builder.setBolt(REVISION_WRITER_BOLT, new IndexingRevisionWriter(ecloudMcsAddress, SUCCESS_MESSAGE),
                getAnInt(REVISION_WRITER_BOLT_PARALLEL))
                .setNumTasks(getAnInt(REVISION_WRITER_BOLT_NUMBER_OF_TASKS))
                .customGrouping(INDEXING_BOLT, new ShuffleGrouping());

        builder.setBolt(NOTIFICATION_BOLT, new IndexingNotificationBolt(topologyProperties.getProperty(CASSANDRA_HOSTS),
                        getAnInt(CASSANDRA_PORT),
                        topologyProperties.getProperty(CASSANDRA_KEYSPACE_NAME),
                        topologyProperties.getProperty(CASSANDRA_USERNAME),
                        topologyProperties.getProperty(CASSANDRA_SECRET_TOKEN),
                        topologyProperties.getProperty(DPS_URL),
                        topologyProperties.getProperty(INDEXING_TOPOLOGY_NAME,"indexer")),
                getAnInt(NOTIFICATION_BOLT_PARALLEL))
                .setNumTasks(
                        getAnInt(NOTIFICATION_BOLT_NUMBER_OF_TASKS))
                .fieldsGrouping(SPOUT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(RETRIEVE_FILE_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(INDEXING_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(REVISION_WRITER_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName));

        return builder.createTopology();
    }

    public static void main(String[] args) {
        try {
            LOGGER.info("Assembling '{}'", TopologiesNames.INDEXING_TOPOLOGY);

            if (args.length <= 2) {
                String providedPropertyFile = (args.length > 0 ? args[0] : "");
                String providedIndexingPropertiesFile = (args.length == 2 ? args[1] : "");

                IndexingTopology indexingTopology =
                        new IndexingTopology(TOPOLOGY_PROPERTIES_FILE, providedPropertyFile,
                                INDEXING_PROPERTIES_FILE, providedIndexingPropertiesFile);

                StormTopology stormTopology = indexingTopology.buildTopology();

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

    private static int getAnInt(String propertyName) {
        return Integer.parseInt(topologyProperties.getProperty(propertyName));
    }
}
