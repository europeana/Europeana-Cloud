package eu.europeana.cloud.service.dps.storm.topologies.oaipmh;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CATEGORIZATION_BOLT_NUMBER_OF_TASKS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CATEGORIZATION_BOLT_PARALLEL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.DUPLICATES_BOLT_NUMBER_OF_TASKS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.DUPLICATES_BOLT_PARALLEL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.MCS_URL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.RECORD_HARVESTING_BOLT_NUMBER_OF_TASKS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.RECORD_HARVESTING_BOLT_PARALLEL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.TOPOLOGY_NAME;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.TOPOLOGY_USER_NAME;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.TOPOLOGY_USER_PASSWORD;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.DUPLICATES_DETECTOR_BOLT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.RECORD_CATEGORIZATION_BOLT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.RECORD_HARVESTING_BOLT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.buildConfig;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.createCassandraProperties;

import eu.europeana.cloud.harvesting.DuplicatedRecordsProcessorBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.io.ECloudTopologyPipeline;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.OaiHarvestedRecordCategorizationBolt;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.RecordHarvestingBolt;
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
 * OAIPMHHarvestingTopology main class where its definition is formulated
 */
public class OAIPMHHarvestingTopology {

  private static final Properties topologyProperties = new Properties();
  private static final String TOPOLOGY_PROPERTIES_FILE = "oai-topology-config.properties";
  private static final Logger LOGGER = LoggerFactory.getLogger(OAIPMHHarvestingTopology.class);

  /**
   * Main constructor for the {@link OAIPMHHarvestingTopology} class
   * @param defaultPropertyFile   location of the file containing default topology properties
   * @param providedPropertyFile  location of the file containing topology properties that will override default properties
   */
  public OAIPMHHarvestingTopology(String defaultPropertyFile, String providedPropertyFile) {
    PropertyFileLoader.loadPropertyFile(defaultPropertyFile, providedPropertyFile, topologyProperties);
    TopologyPropertiesValidator.validateFor(TopologiesNames.OAI_TOPOLOGY, topologyProperties);
  }

  /**
   * Builds actual topology
   * @return created topology definition
   */
  public final StormTopology buildTopology() {
    return new ECloudTopologyPipeline(TopologiesNames.OAI_TOPOLOGY, topologyProperties)
        .addBolt(RECORD_HARVESTING_BOLT, new RecordHarvestingBolt(createCassandraProperties(topologyProperties)),
            RECORD_HARVESTING_BOLT_PARALLEL, RECORD_HARVESTING_BOLT_NUMBER_OF_TASKS)
        .addBolt(RECORD_CATEGORIZATION_BOLT, new OaiHarvestedRecordCategorizationBolt(
                createCassandraProperties(topologyProperties)), CATEGORIZATION_BOLT_PARALLEL,
            CATEGORIZATION_BOLT_NUMBER_OF_TASKS)
        .addHarvestingWriteRecordBolt()
        .addRevisionWriterBoltForHarvesting()
        .addBolt(DUPLICATES_DETECTOR_BOLT, new DuplicatedRecordsProcessorBolt(
                createCassandraProperties(topologyProperties),
                topologyProperties.getProperty(MCS_URL),
                topologyProperties.getProperty(TOPOLOGY_USER_NAME),
                topologyProperties.getProperty(TOPOLOGY_USER_PASSWORD)), DUPLICATES_BOLT_PARALLEL,
            DUPLICATES_BOLT_NUMBER_OF_TASKS, NotificationTuple.TASK_ID_FIELD_NAME)
        .buildTopology();
  }

  /**
   * Method executed by the Storm engine while deploying topology on the cluster
   * @param args list of all needed arguments (in thi case: location of the properties file)
   */
  public static void main(String[] args) {
    try {
      LOGGER.info("Assembling '{}'", TopologiesNames.OAI_TOPOLOGY);
      if (args.length <= 1) {
        String providedPropertyFile = (args.length == 1 ? args[0] : "");

        OAIPMHHarvestingTopology oaipmhHarvestingTopology =
            new OAIPMHHarvestingTopology(TOPOLOGY_PROPERTIES_FILE, providedPropertyFile);

        StormTopology stormTopology = oaipmhHarvestingTopology.buildTopology();
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
