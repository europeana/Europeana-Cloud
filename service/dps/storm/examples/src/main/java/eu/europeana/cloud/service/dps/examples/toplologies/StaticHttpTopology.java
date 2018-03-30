package eu.europeana.cloud.service.dps.examples.toplologies;

import eu.europeana.cloud.service.dps.examples.StaticDpsTaskSpout;
import eu.europeana.cloud.service.dps.examples.toplologies.builder.SimpleStaticHTTPTopologyBuilder;
import eu.europeana.cloud.service.dps.examples.util.DpsTaskUtil;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.utils.Utils;

import java.util.Arrays;

import static eu.europeana.cloud.service.dps.examples.toplologies.constants.TopologyConstants.*;

/**
 * Created by Tarek on 10/2/2017.
 */
public class StaticHttpTopology {
    public static void main(String[] args) {
        StaticDpsTaskSpout staticDpsTaskSpout = new StaticDpsTaskSpout(DpsTaskUtil.generateDPSTaskForHTTP());
        StormTopology stormTopology = SimpleStaticHTTPTopologyBuilder.buildTopology(staticDpsTaskSpout, UIS_URL, MCS_URL);
        Config conf = new Config();
        conf.setDebug(true);
        conf.put(Config.TOPOLOGY_DEBUG, true);
        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS,36000);
        conf.put(INPUT_ZOOKEEPER_ADDRESS,
                INPUT_ZOOKEEPER_PORT);
        conf.put(Config.STORM_ZOOKEEPER_SERVERS,
                Arrays.asList(STORM_ZOOKEEPER_ADDRESS));

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, stormTopology);
        Utils.sleep(60000);
        cluster.killTopology("test");
        cluster.shutdown();

    }
}
