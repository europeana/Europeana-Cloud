package eu.europeana.cloud.service.dps.examples.toplologies;


import eu.europeana.cloud.service.dps.examples.StaticDpsTaskSpout;
import eu.europeana.cloud.service.dps.examples.toplologies.builder.SimpleStaticTopologyBuilder;
import eu.europeana.cloud.service.dps.examples.util.DpsTaskUtil;
import eu.europeana.cloud.service.dps.storm.utils.TopologyHelper;
import eu.europeana.cloud.service.dps.storm.xslt.XsltBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.utils.Utils;

import java.util.Arrays;

import static eu.europeana.cloud.service.dps.examples.toplologies.constants.TopologyConstants.*;

public class StaticXsltTopology {

    public static void main(String[] args) throws Exception {

        StaticDpsTaskSpout staticDpsTaskSpout = new StaticDpsTaskSpout(DpsTaskUtil.generateDpsTaskForXSLT());
        StormTopology stormTopology = SimpleStaticTopologyBuilder.buildTopology(staticDpsTaskSpout, new XsltBolt(), TopologyHelper.XSLT_BOLT,  MCS_URL);

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
