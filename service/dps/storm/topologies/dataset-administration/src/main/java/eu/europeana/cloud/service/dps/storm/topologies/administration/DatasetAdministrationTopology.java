package eu.europeana.cloud.service.dps.storm.topologies.administration;

import eu.europeana.cloud.service.dps.storm.io.ECloudTopologyPipeline;
import eu.europeana.cloud.service.dps.storm.topologies.properties.PropertyFileLoader;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
import eu.europeana.cloud.service.dps.storm.utils.TopologyPropertiesValidator;
import eu.europeana.cloud.service.dps.storm.utils.TopologySubmitter;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.*;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.*;

public class DatasetAdministrationTopology {

  private static final Properties topologyProperties = new Properties();
  private static final String TOPOLOGY_PROPERTIES_FILE = "dataset-administration-topology-config.properties";
  private static final Logger LOGGER = LoggerFactory.getLogger(DatasetAdministrationTopology.class);

  public DatasetAdministrationTopology(String defaultPropertyFile, String providedPropertyFile) {
    PropertyFileLoader.loadPropertyFile(defaultPropertyFile, providedPropertyFile, topologyProperties);
    TopologyPropertiesValidator.validateFor(TopologiesNames.DATASET_ADMINISTRATION_TOPOLOGY, topologyProperties);
  }

  public static void main(String[] args) {
    try {
      LOGGER.info("Assembling '{}'", TopologiesNames.DATASET_ADMINISTRATION_TOPOLOGY);

      if (args.length <= 1) {
        String providedPropertyFile = (args.length == 1 ? args[0] : "");

        DatasetAdministrationTopology datasetAdministrationTopology = new DatasetAdministrationTopology(TOPOLOGY_PROPERTIES_FILE,
            providedPropertyFile);

        StormTopology stormTopology = datasetAdministrationTopology.buildTopology();
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
    return new ECloudTopologyPipeline(TopologiesNames.DATASET_ADMINISTRATION_TOPOLOGY, topologyProperties)
            .addReadFileBolt()
            .addBolt(DUMMY_BOLT, new DummyBolt(createCassandraProperties(topologyProperties)),
                    DUMMY_BOLT_PARALLEL, DUMMY_BOLT_NUMBER_OF_TASKS)
            .addWriteRecordBolt(true)
            .addRevisionWriterBolt()
            .buildTopology();
  }
}
