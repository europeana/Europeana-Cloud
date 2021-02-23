package eu.europeana.cloud.service.dps.storm.topologies.oaipmh;

import eu.europeana.cloud.service.dps.storm.spout.ECloudSpout;
import eu.europeana.cloud.service.dps.storm.topologies.properties.PropertyFileLoader;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
import eu.europeana.cloud.service.dps.storm.utils.TopologyHelper;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.grouping.ShuffleGrouping;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.TOPOLOGY_NAME;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.SPOUT;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.buildConfig;

/**
 *
 */
public class EcloudSpoutTestingTopology {
    private static Properties topologyProperties=new Properties();
    private static final String TOPOLOGY_PROPERTIES_FILE = "ecloud-spout-testing-topology.properties";
    private static final Logger LOGGER = LoggerFactory.getLogger(EcloudSpoutTestingTopology.class);

    public EcloudSpoutTestingTopology(String defaultPropertyFile, String providedPropertyFile) {
        PropertyFileLoader.loadPropertyFile(defaultPropertyFile, providedPropertyFile, topologyProperties);
    }

    public final StormTopology buildTopology() {
        TopologyBuilder builder = new TopologyBuilder();

        ECloudSpout eCloudSpout = TopologyHelper.createECloudSpout(
                "spout_testing_topology", topologyProperties, KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE);

        builder.setSpout(SPOUT, eCloudSpout, 1);

        builder.setBolt("TESTING_BOLT", new TestingBolt(),
                10)
                .setNumTasks(10)
                .customGrouping(SPOUT, new ShuffleGrouping());
        return builder.createTopology();
    }

    public static void main(String[] args) {
        try {
            LOGGER.info("Assembling '{}'", TopologiesNames.OAI_TOPOLOGY);
            if (args.length <= 1) {
                String providedPropertyFile = (args.length == 1 ? args[0] : "");

                EcloudSpoutTestingTopology oaiphmHarvestingTopology =
                        new EcloudSpoutTestingTopology(TOPOLOGY_PROPERTIES_FILE, providedPropertyFile);

                StormTopology stormTopology = oaiphmHarvestingTopology.buildTopology();
                Config config = buildConfig(topologyProperties);

                LOGGER.info("Submitting '{}'...", topologyProperties.getProperty(TOPOLOGY_NAME));
                config.put(Config.TOPOLOGY_SPOUT_WAIT_STRATEGY,"eu.europeana.cloud.service.dps.storm.topologies.oaipmh.FastSleepSpoutWaitStrategy");
//                config.put(Config.TOPOLOGY_SLEEP_SPOUT_WAIT_STRATEGY_TIME_MS, 0);
                new LocalCluster().submitTopology(topologyProperties.getProperty(TOPOLOGY_NAME), config, stormTopology);
            } else {
                LOGGER.error("Invalid number of parameters");
            }
        } catch (Exception e) {
            LOGGER.error("General error while setting up topology", e);
        }
    }
}
