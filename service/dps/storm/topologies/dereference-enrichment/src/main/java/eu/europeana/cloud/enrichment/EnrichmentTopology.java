package eu.europeana.cloud.enrichment;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.DEREFERENCE_SERVICE_URL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.ENRICHMENT_BOLT_NUMBER_OF_TASKS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.ENRICHMENT_BOLT_PARALLEL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.ENRICHMENT_ENTITY_API_KEY;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.ENRICHMENT_ENTITY_API_URL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.ENRICHMENT_ENTITY_MANAGEMENT_URL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.TOPOLOGY_NAME;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.ENRICHMENT_BOLT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.buildConfig;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.createCassandraProperties;

import eu.europeana.cloud.enrichment.bolts.EnrichmentBolt;
import eu.europeana.cloud.service.dps.storm.io.ECloudTopologyBuilder;
import eu.europeana.cloud.service.dps.storm.topologies.properties.PropertyFileLoader;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
import eu.europeana.cloud.service.dps.storm.utils.TopologyPropertiesValidator;
import eu.europeana.cloud.service.dps.storm.utils.TopologySubmitter;
import java.util.Properties;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Tarek on 1/24/2018.
 */
public class EnrichmentTopology {

  private static final Properties topologyProperties = new Properties();
  private static final String TOPOLOGY_PROPERTIES_FILE = "enrichment-topology-config.properties";
  private static final Logger LOGGER = LoggerFactory.getLogger(EnrichmentTopology.class);

  public EnrichmentTopology(String defaultPropertyFile, String providedPropertyFile) {
    PropertyFileLoader.loadPropertyFile(defaultPropertyFile, providedPropertyFile, topologyProperties);
    TopologyPropertiesValidator.validateFor(TopologiesNames.ENRICHMENT_TOPOLOGY, topologyProperties);
  }

  public StormTopology buildTopology() {
    return new ECloudTopologyBuilder(TopologiesNames.NORMALIZATION_TOPOLOGY, topologyProperties)
        .addReadFileBolt()
        .addBolt(ENRICHMENT_BOLT, new EnrichmentBolt(createCassandraProperties(topologyProperties),
                topologyProperties.getProperty(DEREFERENCE_SERVICE_URL),
                topologyProperties.getProperty(ENRICHMENT_ENTITY_MANAGEMENT_URL),
                topologyProperties.getProperty(ENRICHMENT_ENTITY_API_URL),
                topologyProperties.getProperty(ENRICHMENT_ENTITY_API_KEY)),
            ENRICHMENT_BOLT_PARALLEL, ENRICHMENT_BOLT_NUMBER_OF_TASKS
        )
        .addWriteRecordBolt()
        .addRevisionWriterBolt()
        .build();
  }

  public static void main(String[] args) {
    try {
      LOGGER.info("Assembling '{}'", TopologiesNames.ENRICHMENT_TOPOLOGY);

      if (args.length <= 1) {
        String providedPropertyFile = (args.length == 1 ? args[0] : "");

        EnrichmentTopology enrichmentTopology =
            new EnrichmentTopology(TOPOLOGY_PROPERTIES_FILE, providedPropertyFile);

        StormTopology stormTopology = enrichmentTopology.buildTopology();
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

