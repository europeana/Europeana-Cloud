package eu.europeana.cloud.service.dps.examples.toplologies;

import eu.europeana.cloud.service.dps.examples.StaticDpsTaskSpout;
import eu.europeana.cloud.service.dps.examples.toplologies.builder.SimpleStaticHTTPTopologyBuilder;
import eu.europeana.cloud.service.dps.examples.util.DpsTaskUtil;
import eu.europeana.cloud.service.dps.examples.util.TopologyConfigBuilder;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.utils.Utils;


import static eu.europeana.cloud.service.dps.examples.toplologies.constants.TopologyConstants.*;

/**
 * Created by Tarek on 10/2/2017.
 */
public class StaticHttpTopology {
    public static final String TOPOLOGY_NAME = "http_topology";
    public static void main(String[] args) {
        StaticDpsTaskSpout staticDpsTaskSpout = new StaticDpsTaskSpout(DpsTaskUtil.generateDPSTaskForHTTP(),TOPOLOGY_NAME);
        StormTopology stormTopology = SimpleStaticHTTPTopologyBuilder.buildTopology(staticDpsTaskSpout, UIS_URL, MCS_URL);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, TopologyConfigBuilder.buildConfig(), stormTopology);
        Utils.sleep(60000);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();

    }
}
