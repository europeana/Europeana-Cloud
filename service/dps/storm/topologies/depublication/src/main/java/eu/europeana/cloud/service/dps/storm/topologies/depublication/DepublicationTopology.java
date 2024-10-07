package eu.europeana.cloud.service.dps.storm.topologies.depublication;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_HOSTS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_KEYSPACE_NAME;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_PORT;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_SECRET_TOKEN;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_USERNAME;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.DEPUBLICATION_BOLT_NUMBER_OF_TASKS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.DEPUBLICATION_BOLT_PARALLEL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.NOTIFICATION_BOLT_NUMBER_OF_TASKS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.NOTIFICATION_BOLT_PARALLEL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.TOPOLOGY_NAME;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.DEPUBLICATION_BOLT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.NOTIFICATION_BOLT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.buildConfig;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.createCassandraProperties;

import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.topologies.properties.PropertyFileLoader;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
import eu.europeana.cloud.service.dps.storm.utils.TopologyHelper;
import eu.europeana.cloud.service.dps.storm.utils.TopologyPropertiesValidator;
import eu.europeana.cloud.service.dps.storm.utils.TopologySubmitter;
import java.util.List;
import java.util.Properties;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DepublicationTopology {

  private static final Properties topologyProperties = new Properties();
  private static final Properties indexingProperties = new Properties();
  private static final String TOPOLOGY_PROPERTIES_FILE = "depublication-topology-config.properties";
  private static final Logger LOGGER = LoggerFactory.getLogger(DepublicationTopology.class);

  public DepublicationTopology(String defaultPropertyFile, String providedPropertyFile, String providedIndexingPropertiesFile) {
    PropertyFileLoader.loadPropertyFile(defaultPropertyFile, providedPropertyFile, topologyProperties);
    PropertyFileLoader.loadPropertyFile("", providedIndexingPropertiesFile, indexingProperties);
    TopologyPropertiesValidator.validateFor(TopologiesNames.DEPUBLICATION_TOPOLOGY, topologyProperties);
  }

  public static void main(String[] args) {
    try {
      LOGGER.info("Assembling '{}'", TopologiesNames.DEPUBLICATION_TOPOLOGY);

      if (args.length <= 2) {
        String providedPropertyFile = (args.length > 0 ? args[0] : "");
        String providedIndexingPropertiesFile = (args.length == 2 ? args[1] : "");

        DepublicationTopology depublicationTopology = new DepublicationTopology(TOPOLOGY_PROPERTIES_FILE,
            providedPropertyFile, providedIndexingPropertiesFile);

        StormTopology stormTopology = depublicationTopology.buildTopology();
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

  public final StormTopology buildTopology() {
    TopologyBuilder builder = new TopologyBuilder();

    List<String> spoutNames = TopologyHelper.addSpouts(builder, TopologiesNames.DEPUBLICATION_TOPOLOGY, topologyProperties);

    TopologyHelper.addSpoutShuffleGrouping(spoutNames,
        builder.setBolt(DEPUBLICATION_BOLT,
                   new DepublicationBolt(createCassandraProperties(topologyProperties), indexingProperties),
                   (getAnInt(DEPUBLICATION_BOLT_PARALLEL)))
               .setNumTasks((getAnInt(DEPUBLICATION_BOLT_NUMBER_OF_TASKS))));

    TopologyHelper.addSpoutsGroupingToNotificationBolt(spoutNames,
        builder.setBolt(NOTIFICATION_BOLT, new NotificationBolt(topologyProperties.getProperty(CASSANDRA_HOSTS),
                       Integer.parseInt(topologyProperties.getProperty(CASSANDRA_PORT)),
                       topologyProperties.getProperty(CASSANDRA_KEYSPACE_NAME),
                       topologyProperties.getProperty(CASSANDRA_USERNAME),
                       topologyProperties.getProperty(CASSANDRA_SECRET_TOKEN)),
                   getAnInt(NOTIFICATION_BOLT_PARALLEL))
               .setNumTasks(
                   (getAnInt(NOTIFICATION_BOLT_NUMBER_OF_TASKS)))
               .fieldsGrouping(DEPUBLICATION_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                   new Fields(NotificationTuple.TASK_ID_FIELD_NAME)));

    return builder.createTopology();
  }
}
