package eu.europeana.cloud.service.dps.storm.topologies.xslt;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.TOPOLOGY_NAME;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.XSLT_BOLT_NUMBER_OF_TASKS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.XSLT_BOLT_PARALLEL;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.XSLT_BOLT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.buildConfig;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.createCassandraProperties;

import eu.europeana.cloud.service.dps.storm.io.ECloudTopologyPipeline;
import eu.europeana.cloud.service.dps.storm.topologies.properties.PropertyFileLoader;
import eu.europeana.cloud.service.dps.storm.topologies.xslt.bolt.XsltBolt;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
import eu.europeana.cloud.service.dps.storm.utils.TopologyPropertiesValidator;
import eu.europeana.cloud.service.dps.storm.utils.TopologySubmitter;
import java.util.Properties;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is the XSLT transformation topology for Apache Storm. The topology reads from the cloud, download an XSLT sheet from a
 * remote server, apply it to each record read and save it back to the cloud.
 *
 * @author Franco Maria Nardini (francomaria.nardini@isti.cnr.it)
 */
public class XSLTTopology {

  private static final Properties topologyProperties = new Properties();
  private static final String TOPOLOGY_PROPERTIES_FILE = "xslt-topology-config.properties";

  private static final Logger LOGGER = LoggerFactory.getLogger(XSLTTopology.class);

  public XSLTTopology(String defaultPropertyFile, String providedPropertyFile) {
    PropertyFileLoader.loadPropertyFile(defaultPropertyFile, providedPropertyFile, topologyProperties);
    TopologyPropertiesValidator.validateFor(TopologiesNames.XSLT_TOPOLOGY, topologyProperties);
  }

  public StormTopology buildTopology() {
    return new ECloudTopologyPipeline(TopologiesNames.XSLT_TOPOLOGY, topologyProperties)
        .addReadFileBolt()
        .addBolt(XSLT_BOLT, new XsltBolt(createCassandraProperties(topologyProperties)),
            XSLT_BOLT_PARALLEL, XSLT_BOLT_NUMBER_OF_TASKS)
        .addWriteRecordBolt("xslt_topology")
        .addRevisionWriterBolt()
        .buildTopology();
  }

  public static void main(String[] args) {
    try {
      LOGGER.info("Assembling '{}'", TopologiesNames.XSLT_TOPOLOGY);
      if (args.length <= 1) {
        String providedPropertyFile = (args.length == 1 ? args[0] : "");

        XSLTTopology xsltTopology = new XSLTTopology(TOPOLOGY_PROPERTIES_FILE, providedPropertyFile);

        StormTopology stormTopology = xsltTopology.buildTopology();
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
