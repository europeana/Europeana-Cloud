package eu.europeana.cloud.http;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.*;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.DUPLICATES_BOLT_NUMBER_OF_TASKS;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.*;

import eu.europeana.cloud.harvesting.DuplicatedRecordsProcessorBolt;
import eu.europeana.cloud.http.bolts.HttpHarvestedRecordCategorizationBolt;
import eu.europeana.cloud.http.bolts.HttpHarvestingBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
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

/**
 * HttpHarvestingTopology main class where its definition is formulated
 * Created by Tarek on 3/22/2018.
 */
public class HTTPHarvestingTopology {

  private static final Properties topologyProperties = new Properties();
  private static final String TOPOLOGY_PROPERTIES_FILE = "http-topology-config.properties";
  private static final Logger LOGGER = LoggerFactory.getLogger(HTTPHarvestingTopology.class);

  /**
   *Main constructor for the {@link HTTPHarvestingTopology} class
   *
   * @param defaultPropertyFile   location of the file containing default topology properties
   * @param providedPropertyFile  location of the file containing topology properties that will override default properties
   */
  public HTTPHarvestingTopology(String defaultPropertyFile, String providedPropertyFile) {
    PropertyFileLoader.loadPropertyFile(defaultPropertyFile, providedPropertyFile, topologyProperties);
    TopologyPropertiesValidator.validateFor(TopologiesNames.HTTP_TOPOLOGY, topologyProperties);
  }

  /**
   * Builds actual topology
   * @return created topology definition
   */
  public final StormTopology buildTopology() {
    return new ECloudTopologyPipeline(TopologiesNames.HTTP_TOPOLOGY, topologyProperties)
        .addBolt(RECORD_HARVESTING_BOLT, new HttpHarvestingBolt(createCassandraProperties(topologyProperties)),
            RECORD_HARVESTING_BOLT_PARALLEL, RECORD_HARVESTING_BOLT_NUMBER_OF_TASKS)
        .addBolt(RECORD_CATEGORIZATION_BOLT,
            new HttpHarvestedRecordCategorizationBolt(createCassandraProperties(topologyProperties)),
            CATEGORIZATION_BOLT_PARALLEL,
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
      LOGGER.info("Assembling '{}'", TopologiesNames.HTTP_TOPOLOGY);
      if (args.length <= 1) {
        String providedPropertyFile = (args.length == 1 ? args[0] : "");

        HTTPHarvestingTopology httpHarvestingTopology = new HTTPHarvestingTopology(TOPOLOGY_PROPERTIES_FILE,
            providedPropertyFile);

        StormTopology stormTopology = httpHarvestingTopology.buildTopology();
        Config config = buildConfig(topologyProperties);
        LOGGER.info("Submitting '{}'...", topologyProperties.getProperty(TOPOLOGY_NAME));
        TopologySubmitter.submitTopology(topologyProperties.getProperty(TOPOLOGY_NAME), config, stormTopology);
      } else {
        LOGGER.error("Invalid number of parameters");
      }
    } catch (Exception e) {
      LOGGER.error("General error in HTTP harvesting topology", e);
    }
  }
}
