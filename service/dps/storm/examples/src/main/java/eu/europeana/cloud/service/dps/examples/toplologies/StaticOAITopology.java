package eu.europeana.cloud.service.dps.examples.toplologies;

import eu.europeana.cloud.service.dps.examples.StaticDpsTaskSpout;
import eu.europeana.cloud.service.dps.examples.toplologies.builder.SimpleStaticOAITopologyBuilder;
import eu.europeana.cloud.service.dps.examples.util.DpsTaskUtil;
import eu.europeana.cloud.service.dps.examples.util.TopologyConfigBuilder;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.utils.Utils;


import static eu.europeana.cloud.service.dps.examples.toplologies.constants.TopologyConstants.UIS_URL;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.*;


/**
 * Created by Tarek on 10/2/2017.
 */
public class StaticOAITopology {
    public static final String TOPOLOGY_NAME = "oai_topology";

    public static void main(String[] args) {
        StaticDpsTaskSpout staticDpsTaskSpout = new StaticDpsTaskSpout(DpsTaskUtil.generateDPsTaskForOAI(), TOPOLOGY_NAME);
        StormTopology stormTopology = SimpleStaticOAITopologyBuilder.buildTopology(staticDpsTaskSpout, UIS_URL, MCS_URL);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, TopologyConfigBuilder.buildConfig(), stormTopology);
        Utils.sleep(60000);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();

    }

}
