package eu.europeana.cloud.service.dps.storm.topologies.spoutest;

import eu.europeana.cloud.service.dps.storm.topologies.properties.PropertyFileLoader;
import eu.europeana.cloud.service.dps.storm.utils.TopologyHelper;
import eu.europeana.cloud.service.dps.storm.utils.TopologySubmitter;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.TOPOLOGY_NAME;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.buildConfig;

/**
 *
 */
public class EcloudSpoutTestingTopology {
    private static Properties topologyProperties = new Properties();
    private static final String TOPOLOGY_PROPERTIES_FILE = "ecloud-spout-testing-topology.properties";
    private static final Logger LOGGER = LoggerFactory.getLogger(EcloudSpoutTestingTopology.class);

    public EcloudSpoutTestingTopology(String defaultPropertyFile, String providedPropertyFile) {
        PropertyFileLoader.loadPropertyFile(defaultPropertyFile, providedPropertyFile, topologyProperties);
    }

    public final StormTopology buildTopology() {
        TopologyBuilder builder = new TopologyBuilder();

        List<String> spoutNames = TopologyHelper.addSpouts(builder, "spout_testing_topology", topologyProperties);

        TopologyHelper.addSpoutShuffleGrouping(spoutNames,
                builder.setBolt("TESTING_BOLT", new TestingBolt(),
                                16)
                        .setNumTasks(16)
        );
//        TopologyHelper.addSpoutFieldGrouping(
//                builder.setBolt("TESTING_BOLT", new TestingBolt(),
//                                10)
//                        .setNumTasks(10),spoutNames, StormTupleKeys.INPUT_FILES_TUPLE_KEY
//        );
        return builder.createTopology();
    }

    public static void main(String[] args) {
        try {
            LOGGER.info("Assembling '{}'", "spout_testing_topology");
            if (args.length <= 1) {
                String providedPropertyFile = (args.length == 1 ? args[0] : "");

                EcloudSpoutTestingTopology topology = new EcloudSpoutTestingTopology(TOPOLOGY_PROPERTIES_FILE, providedPropertyFile);

                StormTopology stormTopology = topology.buildTopology();
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
