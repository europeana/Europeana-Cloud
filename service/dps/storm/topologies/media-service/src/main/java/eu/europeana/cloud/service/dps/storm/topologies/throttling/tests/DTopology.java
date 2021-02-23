package eu.europeana.cloud.service.dps.storm.topologies.throttling.tests;

import eu.europeana.cloud.service.dps.storm.StormTupleKeys;
import eu.europeana.cloud.service.dps.storm.topologies.properties.PropertyFileLoader;
import eu.europeana.cloud.service.dps.storm.utils.ThrottlingShuffleGrouping;
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
import static eu.europeana.cloud.service.dps.storm.topologies.throttling.tests.ATopology.THROTTLING_TESTING_TOPOLOGY_NAME;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.buildConfig;

/**
 *  The same as CTopology, topology with two throttled bolts, smilar like in media topology, but here is used our own
 *  custom grouping: ThrottlingShuffleGrouping.
 */
public class DTopology {

    private static Properties topologyProperties = new Properties();
    private static final String TOPOLOGY_PROPERTIES_FILE = "d-topology.properties";
    private static final Logger LOGGER = LoggerFactory.getLogger(DTopology.class);

    public DTopology(String defaultPropertyFile, String providedPropertyFile) {
        PropertyFileLoader.loadPropertyFile(defaultPropertyFile, providedPropertyFile, topologyProperties);
    }

    public final StormTopology buildTopology() {
        TopologyBuilder builder = new TopologyBuilder();

        //List<String> spoutNames = TopologyHelper.addSpouts(builder, A_TOPOLOGY, topologyProperties);
        List<String> spoutNames = TopologyHelper.addMediaSpouts(builder, THROTTLING_TESTING_TOPOLOGY_NAME, topologyProperties);

        int aBoltParallel = Integer.parseInt(topologyProperties.getProperty("A_BOLT_PARALLEL"));
        int bBoltParallel = Integer.parseInt(topologyProperties.getProperty("B_BOLT_PARALLEL"));
        //TopologyHelper.addSpoutOwnGrouping(spoutNames, builder.setBolt("TESTING_BOLT", new TestingIntermediateBolt(), aBoltParallel));
        TopologyHelper.addSpoutThrottlingGrouping(spoutNames, builder.setBolt("TESTING_BOLT_A", new TestingIntermediateBolt(10), aBoltParallel),
                StormTupleKeys.THROTTLING_GROUPING_ATTRIBUTE);

        builder.setBolt("TESTING_BOLT_B",new TestingBolt(12773), bBoltParallel)
                .customGrouping("TESTING_BOLT_A", new ThrottlingShuffleGrouping());
        return builder.createTopology();
    }

    public static void main(String[] args) {
        try {
            LOGGER.info("Assembling '{}'", THROTTLING_TESTING_TOPOLOGY_NAME);
            if (args.length <= 1) {
                String providedPropertyFile = (args.length == 1 ? args[0] : "");

                DTopology topology = new DTopology(TOPOLOGY_PROPERTIES_FILE, providedPropertyFile);

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
