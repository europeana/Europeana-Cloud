package eu.europeana.cloud.service.dps.storm.topologies.indexing;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_HOSTS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_KEYSPACE_NAME;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_PORT;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_SECRET_TOKEN;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_USERNAME;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.INDEXING_BOLT_NUMBER_OF_TASKS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.INDEXING_BOLT_PARALLEL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.MCS_URL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.NOTIFICATION_BOLT_NUMBER_OF_TASKS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.NOTIFICATION_BOLT_PARALLEL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.RETRIEVE_FILE_BOLT_NUMBER_OF_TASKS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.RETRIEVE_FILE_BOLT_PARALLEL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.REVISION_WRITER_BOLT_NUMBER_OF_TASKS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.REVISION_WRITER_BOLT_PARALLEL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.TOPOLOGY_NAME;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.TOPOLOGY_USER_NAME;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.TOPOLOGY_USER_PASSWORD;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.UIS_URL;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.INDEXING_BOLT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.NOTIFICATION_BOLT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.RETRIEVE_FILE_BOLT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.REVISION_WRITER_BOLT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.buildConfig;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.createCassandraProperties;

import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.io.IndexingRevisionWriter;
import eu.europeana.cloud.service.dps.storm.io.ReadFileBolt;
import eu.europeana.cloud.service.dps.storm.topologies.indexing.bolts.IndexingBolt;
import eu.europeana.cloud.service.dps.storm.topologies.properties.PropertyFileLoader;
import eu.europeana.cloud.service.dps.storm.utils.DbConnectionDetails;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
import eu.europeana.cloud.service.dps.storm.utils.TopologyHelper;
import eu.europeana.cloud.service.dps.storm.utils.TopologyPropertiesValidator;
import eu.europeana.cloud.service.dps.storm.utils.TopologySubmitter;
import java.util.List;
import java.util.Properties;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.grouping.ShuffleGrouping;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Indexing topology main file
 */
public class IndexingTopology {

  private static final Logger LOGGER = LoggerFactory.getLogger(IndexingTopology.class);

  private static final Properties topologyProperties = new Properties();
  private static final Properties indexingProperties = new Properties();
  private static final String TOPOLOGY_PROPERTIES_FILE = "indexing-topology-config.properties";
  public static final String SUCCESS_MESSAGE = "Record is indexed correctly";

  private IndexingTopology(String defaultPropertyFile, String providedPropertyFile, String providedIndexingPropertiesFile) {
    PropertyFileLoader.loadPropertyFile(defaultPropertyFile, providedPropertyFile, topologyProperties);
    PropertyFileLoader.loadPropertyFile("", providedIndexingPropertiesFile, indexingProperties);
    TopologyPropertiesValidator.validateFor(TopologiesNames.INDEXING_TOPOLOGY, topologyProperties);
  }

  private StormTopology buildTopology() {

    TopologyBuilder builder = new TopologyBuilder();
    ReadFileBolt readFileBolt = new ReadFileBolt(
        createCassandraProperties(topologyProperties),
        topologyProperties.getProperty(MCS_URL),
        topologyProperties.getProperty(TOPOLOGY_USER_NAME),
        topologyProperties.getProperty(TOPOLOGY_USER_PASSWORD));

    List<String> spoutNames = TopologyHelper.addSpouts(builder,
        TopologiesNames.INDEXING_TOPOLOGY,
        topologyProperties);

    TopologyHelper.addSpoutShuffleGrouping(spoutNames,
        builder.setBolt(RETRIEVE_FILE_BOLT, readFileBolt, getAnInt(RETRIEVE_FILE_BOLT_PARALLEL))
               .setNumTasks(getAnInt(RETRIEVE_FILE_BOLT_NUMBER_OF_TASKS)));

    builder.setBolt(INDEXING_BOLT, new IndexingBolt(
                   createCassandraProperties(topologyProperties),
                   indexingProperties,
                   topologyProperties.getProperty(UIS_URL),
                   topologyProperties.getProperty(TOPOLOGY_USER_NAME),
                   topologyProperties.getProperty(TOPOLOGY_USER_PASSWORD)
               ),
               getAnInt(INDEXING_BOLT_PARALLEL))
           .setNumTasks(getAnInt(INDEXING_BOLT_NUMBER_OF_TASKS))
           .customGrouping(RETRIEVE_FILE_BOLT, new ShuffleGrouping());

    builder.setBolt(REVISION_WRITER_BOLT, new IndexingRevisionWriter(createCassandraProperties(topologyProperties),
                   topologyProperties.getProperty(MCS_URL),
                   topologyProperties.getProperty(TOPOLOGY_USER_NAME),
                   topologyProperties.getProperty(TOPOLOGY_USER_PASSWORD),
                   SUCCESS_MESSAGE),
               getAnInt(REVISION_WRITER_BOLT_PARALLEL))
           .setNumTasks(getAnInt(REVISION_WRITER_BOLT_NUMBER_OF_TASKS))
           .customGrouping(INDEXING_BOLT, new ShuffleGrouping());

    TopologyHelper.addSpoutsGroupingToNotificationBolt(spoutNames,
        builder.setBolt(NOTIFICATION_BOLT, new NotificationBolt(topologyProperties.getProperty(CASSANDRA_HOSTS),
                       getAnInt(CASSANDRA_PORT),
                       topologyProperties.getProperty(CASSANDRA_KEYSPACE_NAME),
                       topologyProperties.getProperty(CASSANDRA_USERNAME),
                       topologyProperties.getProperty(CASSANDRA_SECRET_TOKEN)),
                   getAnInt(NOTIFICATION_BOLT_PARALLEL))
               .setNumTasks(
                   getAnInt(NOTIFICATION_BOLT_NUMBER_OF_TASKS))
               .fieldsGrouping(RETRIEVE_FILE_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                   new Fields(NotificationTuple.TASK_ID_FIELD_NAME))
               .fieldsGrouping(INDEXING_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                   new Fields(NotificationTuple.TASK_ID_FIELD_NAME))
               .fieldsGrouping(REVISION_WRITER_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                   new Fields(NotificationTuple.TASK_ID_FIELD_NAME)));

    return builder.createTopology();
  }

  public static void main(String[] args) {
    try {
      LOGGER.info("Assembling '{}'", TopologiesNames.INDEXING_TOPOLOGY);

      if (args.length <= 2) {
        String providedPropertyFile = (args.length > 0 ? args[0] : "");
        String providedIndexingPropertiesFile = (args.length == 2 ? args[1] : "");

        IndexingTopology indexingTopology =
            new IndexingTopology(TOPOLOGY_PROPERTIES_FILE, providedPropertyFile, providedIndexingPropertiesFile);

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
