package eu.europeana.cloud.service.dps.storm.topologies.indexing;

import eu.europeana.cloud.service.dps.storm.io.ECloudTopologyPipeline;
import eu.europeana.cloud.service.dps.storm.io.IndexingRevisionWriter;
import eu.europeana.cloud.service.dps.storm.topologies.indexing.bolts.IndexingBolt;
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

/**
 * Indexing topology main file
 */
public final class IndexingTopology {

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
    return new ECloudTopologyPipeline(TopologiesNames.INDEXING_TOPOLOGY, topologyProperties)
            .addReadFileBolt()
            .addBolt(INDEXING_BOLT, new IndexingBolt(
                    createCassandraProperties(topologyProperties),
                    indexingProperties,
                    topologyProperties.getProperty(UIS_URL),
                    topologyProperties.getProperty(TOPOLOGY_USER_NAME),
                    topologyProperties.getProperty(TOPOLOGY_USER_PASSWORD)
            ), INDEXING_BOLT_PARALLEL, INDEXING_BOLT_NUMBER_OF_TASKS)
            .addWriteRecordBolt()
        .addBolt(REVISION_WRITER_BOLT, new IndexingRevisionWriter(createCassandraProperties(topologyProperties),
                topologyProperties.getProperty(MCS_URL),
                topologyProperties.getProperty(TOPOLOGY_USER_NAME),
                topologyProperties.getProperty(TOPOLOGY_USER_PASSWORD),
                SUCCESS_MESSAGE),
            REVISION_WRITER_BOLT_PARALLEL, REVISION_WRITER_BOLT_NUMBER_OF_TASKS)
        .buildTopology();
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
