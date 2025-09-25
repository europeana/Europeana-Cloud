package eu.europeana.cloud.service.dps.storm.topologies.depublication;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.DEPUBLICATION_BOLT_NUMBER_OF_TASKS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.DEPUBLICATION_BOLT_PARALLEL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.TOPOLOGY_NAME;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.DEPUBLICATION_BOLT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.buildConfig;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.createCassandraProperties;

import eu.europeana.cloud.service.dps.storm.io.ECloudTopologyPipeline;
import eu.europeana.cloud.service.dps.storm.topologies.properties.PropertyFileLoader;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
import eu.europeana.cloud.service.dps.storm.utils.TopologyPropertiesValidator;
import eu.europeana.cloud.service.dps.storm.utils.TopologySubmitter;
import java.util.Properties;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
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

  public final StormTopology buildTopology() {
    return new ECloudTopologyPipeline(TopologiesNames.INDEXING_TOPOLOGY, topologyProperties)
        .addBolt(DEPUBLICATION_BOLT, new DepublicationBolt(createCassandraProperties(topologyProperties), indexingProperties),
            DEPUBLICATION_BOLT_PARALLEL, DEPUBLICATION_BOLT_NUMBER_OF_TASKS)
        .addWriteRecordBolt()
        .buildTopology();
  }
}
