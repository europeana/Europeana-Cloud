package eu.europeana.cloud.service.dps.storm.topologies.validation.topology;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_HOSTS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_KEYSPACE_NAME;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_PORT;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_SECRET_TOKEN;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_USERNAME;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.MCS_URL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.NOTIFICATION_BOLT_NUMBER_OF_TASKS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.NOTIFICATION_BOLT_PARALLEL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.RETRIEVE_FILE_BOLT_NUMBER_OF_TASKS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.RETRIEVE_FILE_BOLT_PARALLEL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.REVISION_WRITER_BOLT_NUMBER_OF_TASKS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.REVISION_WRITER_BOLT_PARALLEL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.STATISTICS_BOLT_NUMBER_OF_TASKS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.STATISTICS_BOLT_PARALLEL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.TOPOLOGY_NAME;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.TOPOLOGY_USER_NAME;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.TOPOLOGY_USER_PASSWORD;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.VALIDATION_BOLT_NUMBER_OF_TASKS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.VALIDATION_BOLT_PARALLEL;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.NOTIFICATION_BOLT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.RETRIEVE_FILE_BOLT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.REVISION_WRITER_BOLT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.STATISTICS_BOLT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.VALIDATION_BOLT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.buildConfig;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.createCassandraProperties;
import static java.lang.Integer.parseInt;

import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.io.ReadFileBolt;
import eu.europeana.cloud.service.dps.storm.io.RevisionWriterBolt;
import eu.europeana.cloud.service.dps.storm.topologies.properties.PropertyFileLoader;
import eu.europeana.cloud.service.dps.storm.topologies.validation.topology.bolts.StatisticsBolt;
import eu.europeana.cloud.service.dps.storm.topologies.validation.topology.bolts.ValidationBolt;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
import eu.europeana.cloud.service.dps.storm.utils.TopologyHelper;
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
 * Created by Tarek on 12/5/2017.
 */
public class ValidationTopology {

  private static final Properties topologyProperties = new Properties();
  private static final Properties validationProperties = new Properties();
  private static final String TOPOLOGY_PROPERTIES_FILE = "validation-topology-config.properties";
  private static final String VALIDATION_PROPERTIES_FILE = "validation.properties";
  private static final Logger LOGGER = LoggerFactory.getLogger(ValidationTopology.class);

  public ValidationTopology(String defaultPropertyFile, String providedPropertyFile,
      String defaultValidationPropertiesFile, String providedValidationPropertiesFile) {
    PropertyFileLoader.loadPropertyFile(defaultPropertyFile, providedPropertyFile, topologyProperties);
    PropertyFileLoader.loadPropertyFile(defaultValidationPropertiesFile, providedValidationPropertiesFile, validationProperties);
  }


  public final StormTopology buildTopology() {
    TopologyBuilder builder = new TopologyBuilder();

    List<String> spoutNames = TopologyHelper.addSpouts(builder, TopologiesNames.VALIDATION_TOPOLOGY, topologyProperties);

    ReadFileBolt readFileBolt = new ReadFileBolt(
        createCassandraProperties(topologyProperties),
        topologyProperties.getProperty(MCS_URL),
        topologyProperties.getProperty(TOPOLOGY_USER_NAME),
        topologyProperties.getProperty(TOPOLOGY_USER_PASSWORD));

    TopologyHelper.addSpoutShuffleGrouping(spoutNames,
        builder.setBolt(RETRIEVE_FILE_BOLT, readFileBolt, (getAnInt(RETRIEVE_FILE_BOLT_PARALLEL)))
               .setNumTasks((getAnInt(RETRIEVE_FILE_BOLT_NUMBER_OF_TASKS))));

    builder.setBolt(VALIDATION_BOLT, new ValidationBolt(createCassandraProperties(topologyProperties), validationProperties),
               (getAnInt(VALIDATION_BOLT_PARALLEL)))
           .setNumTasks((getAnInt(VALIDATION_BOLT_NUMBER_OF_TASKS)))
           .customGrouping(RETRIEVE_FILE_BOLT, new ShuffleGrouping());

    builder.setBolt(STATISTICS_BOLT, new StatisticsBolt(createCassandraProperties(topologyProperties),
                   topologyProperties.getProperty(CASSANDRA_HOSTS),
                   Integer.parseInt(topologyProperties.getProperty(CASSANDRA_PORT)),
                   topologyProperties.getProperty(CASSANDRA_KEYSPACE_NAME),
                   topologyProperties.getProperty(CASSANDRA_USERNAME),
                   topologyProperties.getProperty(CASSANDRA_SECRET_TOKEN)),
               (getAnInt(STATISTICS_BOLT_PARALLEL)))
           .setNumTasks((getAnInt(STATISTICS_BOLT_NUMBER_OF_TASKS)))
           .customGrouping(VALIDATION_BOLT, new ShuffleGrouping());

    builder.setBolt(REVISION_WRITER_BOLT, new RevisionWriterBolt(
                   createCassandraProperties(topologyProperties),
                   topologyProperties.getProperty(MCS_URL),
                   topologyProperties.getProperty(TOPOLOGY_USER_NAME),
                   topologyProperties.getProperty(TOPOLOGY_USER_PASSWORD)
               ),
               (getAnInt(REVISION_WRITER_BOLT_PARALLEL)))
           .setNumTasks(
               (getAnInt(REVISION_WRITER_BOLT_NUMBER_OF_TASKS)))
           .customGrouping(STATISTICS_BOLT, new ShuffleGrouping());

    TopologyHelper.addSpoutsGroupingToNotificationBolt(spoutNames,
        builder.setBolt(NOTIFICATION_BOLT, new NotificationBolt(topologyProperties.getProperty(CASSANDRA_HOSTS),
                       Integer.parseInt(topologyProperties.getProperty(CASSANDRA_PORT)),
                       topologyProperties.getProperty(CASSANDRA_KEYSPACE_NAME),
                       topologyProperties.getProperty(CASSANDRA_USERNAME),
                       topologyProperties.getProperty(CASSANDRA_SECRET_TOKEN)),
                   getAnInt(NOTIFICATION_BOLT_PARALLEL))
               .setNumTasks(
                   (getAnInt(NOTIFICATION_BOLT_NUMBER_OF_TASKS)))
               .fieldsGrouping(RETRIEVE_FILE_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                   new Fields(NotificationTuple.TASK_ID_FIELD_NAME))
               .fieldsGrouping(VALIDATION_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                   new Fields(NotificationTuple.TASK_ID_FIELD_NAME))
               .fieldsGrouping(STATISTICS_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                   new Fields(NotificationTuple.TASK_ID_FIELD_NAME))
               .fieldsGrouping(REVISION_WRITER_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                   new Fields(NotificationTuple.TASK_ID_FIELD_NAME)));
    return builder.createTopology();
  }

  private static int getAnInt(String parseTasksBoltParallel) {
    return parseInt(topologyProperties.getProperty(parseTasksBoltParallel));
  }

  public static void main(String[] args) {
    try {
      LOGGER.info("Assembling '{}'", TopologiesNames.VALIDATION_TOPOLOGY);
      if (args.length <= 2) {

        String providedPropertyFile = (args.length > 0 ? args[0] : "");
        String providedValidationPropertiesFile = (args.length == 2 ? args[1] : "");

        ValidationTopology validationTopology =
            new ValidationTopology(TOPOLOGY_PROPERTIES_FILE, providedPropertyFile,
                VALIDATION_PROPERTIES_FILE, providedValidationPropertiesFile);

        StormTopology stormTopology = validationTopology.buildTopology();
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

