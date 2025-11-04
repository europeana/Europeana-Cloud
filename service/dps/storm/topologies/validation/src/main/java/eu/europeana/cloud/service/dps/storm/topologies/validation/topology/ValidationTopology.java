package eu.europeana.cloud.service.dps.storm.topologies.validation.topology;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_HOSTS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_KEYSPACE_NAME;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_PORT;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_SECRET_TOKEN;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_USERNAME;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.STATISTICS_BOLT_NUMBER_OF_TASKS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.STATISTICS_BOLT_PARALLEL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.TOPOLOGY_NAME;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.VALIDATION_BOLT_NUMBER_OF_TASKS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.VALIDATION_BOLT_PARALLEL;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.STATISTICS_BOLT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.VALIDATION_BOLT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.buildConfig;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.createCassandraProperties;

import eu.europeana.cloud.service.dps.storm.io.ECloudTopologyPipeline;
import eu.europeana.cloud.service.dps.storm.topologies.properties.PropertyFileLoader;
import eu.europeana.cloud.service.dps.storm.topologies.validation.topology.bolts.StatisticsBolt;
import eu.europeana.cloud.service.dps.storm.topologies.validation.topology.bolts.ValidationBolt;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
import eu.europeana.cloud.service.dps.storm.utils.TopologyPropertiesValidator;
import eu.europeana.cloud.service.dps.storm.utils.TopologySubmitter;
import java.util.Properties;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
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
    TopologyPropertiesValidator.validateFor(TopologiesNames.VALIDATION_TOPOLOGY, topologyProperties);
  }


  public final StormTopology buildTopology() {
    return new ECloudTopologyPipeline(TopologiesNames.VALIDATION_TOPOLOGY, topologyProperties)
        .addReadFileBolt()
        .addBolt(VALIDATION_BOLT, new ValidationBolt(createCassandraProperties(topologyProperties), validationProperties),
            VALIDATION_BOLT_PARALLEL, VALIDATION_BOLT_NUMBER_OF_TASKS)
        .addBolt(STATISTICS_BOLT, new StatisticsBolt(createCassandraProperties(topologyProperties),
                topologyProperties.getProperty(CASSANDRA_HOSTS), Integer.parseInt(topologyProperties.getProperty(CASSANDRA_PORT)),
                topologyProperties.getProperty(CASSANDRA_KEYSPACE_NAME), topologyProperties.getProperty(CASSANDRA_USERNAME),
                topologyProperties.getProperty(CASSANDRA_SECRET_TOKEN)),
            STATISTICS_BOLT_PARALLEL, STATISTICS_BOLT_NUMBER_OF_TASKS)
        .addWriteRecordBolt("validation__topology")
        .addRevisionWriterBolt()
        .buildTopology();
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
        String topologyName = topologyProperties.getProperty(TOPOLOGY_NAME);
        LOGGER.info("Submitting '{}'...", topologyName);
        TopologySubmitter.submitTopology(topologyName, config, stormTopology);
      } else {
        LOGGER.error("Invalid number of parameters");
      }
    } catch (Exception e) {
      LOGGER.error("General error while setting up topology", e);
    }
  }
}

