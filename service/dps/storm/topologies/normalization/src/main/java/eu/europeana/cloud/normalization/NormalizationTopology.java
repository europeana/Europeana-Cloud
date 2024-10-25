package eu.europeana.cloud.normalization;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.NORMALIZATION_BOLT_NUMBER_OF_TASKS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.NORMALIZATION_BOLT_PARALLEL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.TOPOLOGY_NAME;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.NORMALIZATION_BOLT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.buildConfig;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.createCassandraProperties;

import eu.europeana.cloud.normalization.bolts.NormalizationBolt;
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

public class NormalizationTopology {

  private static final Logger LOGGER = LoggerFactory.getLogger(NormalizationTopology.class);

  private static final String TOPOLOGY_PROPERTIES_FILE = "normalization-topology-config.properties";
  private static final Properties topologyProperties = new Properties();

  public NormalizationTopology(String defaultPropertyFile, String providedPropertyFile) {
    PropertyFileLoader.loadPropertyFile(defaultPropertyFile, providedPropertyFile, topologyProperties);
    TopologyPropertiesValidator.validateFor(TopologiesNames.NORMALIZATION_TOPOLOGY, topologyProperties);
  }

  public StormTopology buildTopology() {
    return new ECloudTopologyPipeline(TopologiesNames.NORMALIZATION_TOPOLOGY, topologyProperties)
        .addReadFileBolt()
        .addBolt(NORMALIZATION_BOLT, new NormalizationBolt(createCassandraProperties(topologyProperties)),
            NORMALIZATION_BOLT_PARALLEL, NORMALIZATION_BOLT_NUMBER_OF_TASKS)
        .addWriteRecordBolt()
        .addRevisionWriterBolt()
        .buildTopology();
  }

  public static void main(String... args) {
    try {
      LOGGER.info("Assembling '{}'", TopologiesNames.NORMALIZATION_TOPOLOGY);
      if (args.length <= 1) {
        String providedPropertyFile = (args.length == 1 ? args[0] : "");

        NormalizationTopology normalizationTopology =
            new NormalizationTopology(TOPOLOGY_PROPERTIES_FILE, providedPropertyFile);

        StormTopology stormTopology = normalizationTopology.buildTopology();
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
