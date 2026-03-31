package eu.europeana.cloud.service.dps.storm.topologies.link.check;

import eu.europeana.cloud.service.dps.storm.StormTupleKeys;
import eu.europeana.cloud.service.dps.storm.io.ECloudTopologyPipeline;
import eu.europeana.cloud.service.dps.storm.io.ParseFileForLinkCheckBolt;
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
 * Created by Tarek on 12/14/2018.
 */
public class LinkCheckTopology {

  private static final Properties topologyProperties = new Properties();
  private static final String TOPOLOGY_PROPERTIES_FILE = "link-check-topology-config.properties";
  private static final Logger LOGGER = LoggerFactory.getLogger(LinkCheckTopology.class);

  public LinkCheckTopology(String defaultPropertyFile, String providedPropertyFile) {
    PropertyFileLoader.loadPropertyFile(defaultPropertyFile, providedPropertyFile, topologyProperties);
    TopologyPropertiesValidator.validateFor(TopologiesNames.LINKCHECK_TOPOLOGY, topologyProperties);
  }

  public final StormTopology buildTopology() {
    return new ECloudTopologyPipeline(TopologiesNames.LINKCHECK_TOPOLOGY, topologyProperties)
        .addBolt(PARSE_FILE_BOLT, new ParseFileForLinkCheckBolt(createCassandraProperties(topologyProperties),
                topologyProperties.getProperty(MCS_URL), topologyProperties.getProperty(TOPOLOGY_USER_NAME),
                topologyProperties.getProperty(TOPOLOGY_USER_PASSWORD)), PARSE_FILE_BOLT_PARALLEL,
            PARSE_FILE_BOLT_BOLT_NUMBER_OF_TASKS)
        .addBolt(LINK_CHECK_BOLT, new LinkCheckBolt(createCassandraProperties(topologyProperties)),
                LINK_CHECK_BOLT_PARALLEL, LINK_CHECK_BOLT_NUMBER_OF_TASKS, StormTupleKeys.INPUT_FILES_TUPLE_FIELD)
        .buildTopology();
  }

  public static void main(String[] args) {
    try {
      LOGGER.info("Assembling '{}'", TopologiesNames.INDEXING_TOPOLOGY);
      if (args.length <= 1) {
        String providedPropertyFile = (args.length == 1 ? args[0] : "");

        LinkCheckTopology linkCheckTopology = new LinkCheckTopology(TOPOLOGY_PROPERTIES_FILE, providedPropertyFile);

        StormTopology stormTopology = linkCheckTopology.buildTopology();
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
