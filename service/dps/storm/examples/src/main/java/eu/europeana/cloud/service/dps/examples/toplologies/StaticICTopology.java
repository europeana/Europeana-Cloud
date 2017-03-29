package eu.europeana.cloud.service.dps.examples.toplologies;

import eu.europeana.cloud.service.dps.examples.StaticDpsTaskSpout;
import eu.europeana.cloud.service.dps.examples.toplologies.builder.SimpleStaticTopologyBuilder;
import eu.europeana.cloud.service.dps.examples.util.DpsTaskUtil;
import eu.europeana.cloud.service.dps.storm.topologies.ic.topology.bolt.IcBolt;
import eu.europeana.cloud.service.dps.storm.utils.TopologyHelper;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.utils.Utils;

import java.util.Arrays;

import static eu.europeana.cloud.service.dps.examples.toplologies.constants.TopologyConstants.*;


/**
 * Example ecloud topology:
 * <p/>
 * - Creates a DpsTask using {@link StaticDpsTaskSpout}
 * <p/>
 * - Reads a File from eCloud
 * <p/>
 * - Writes a File to eCloud
 */
public class StaticICTopology {

    public static void main(String[] args) throws Exception {
        StaticDpsTaskSpout staticDpsTaskSpout = new StaticDpsTaskSpout(DpsTaskUtil.generateDPsTaskForIC());
        StormTopology stormTopology = SimpleStaticTopologyBuilder.buildTopology(staticDpsTaskSpout, new IcBolt(), TopologyHelper.IC_BOLT, MCS_URL);
        Config conf = new Config();
        conf.setDebug(true);
        conf.put(Config.TOPOLOGY_DEBUG, true);
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



