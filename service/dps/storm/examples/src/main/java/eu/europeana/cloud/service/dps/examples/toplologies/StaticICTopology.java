package eu.europeana.cloud.service.dps.examples.toplologies;

import eu.europeana.cloud.service.dps.examples.StaticDpsTaskSpout;
import eu.europeana.cloud.service.dps.examples.toplologies.builder.SimpleStaticTopologyBuilder;
import eu.europeana.cloud.service.dps.examples.toplologies.constants.TopologyConstants;
import eu.europeana.cloud.service.dps.examples.util.DpsTaskUtil;
import eu.europeana.cloud.service.dps.examples.util.TopologyConfigBuilder;
import eu.europeana.cloud.service.dps.storm.topologies.ic.topology.bolt.IcBolt;
import eu.europeana.cloud.service.dps.storm.utils.TopologyHelper;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.utils.Utils;

import java.util.Arrays;

import static eu.europeana.cloud.service.dps.examples.toplologies.constants.TopologyConstants.*;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.MCS_URL;


/**
 * Example for ICTopology to read Datasets:
 * <p/>
 * - Creates a DpsTask using {@link StaticDpsTaskSpout}
 * <p/>
 * - Reads a DataSet/DataSets or part of them using Revisions from eCloud ; Reads Files inside those DataSets , converts those files into jp2;
 * - Writes those jp2 Files to eCloud ; Assigns them to outputRevision in case specified and assigns them to output dataSets in case specified!.
 */
public class StaticICTopology {

    public static final String TOPOLOGY_NAME = "ic_topology";

    public static void main(String[] args) throws Exception {
        StaticDpsTaskSpout staticDpsTaskSpout = new StaticDpsTaskSpout(DpsTaskUtil.generateDPsTaskForIC(), TOPOLOGY_NAME);
        StormTopology stormTopology = SimpleStaticTopologyBuilder.buildTopology(staticDpsTaskSpout, new IcBolt(), TopologyHelper.IC_BOLT, TopologyConstants.MCS_URL);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, TopologyConfigBuilder.buildConfig(), stormTopology);
        Utils.sleep(60000);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();

    }
}



