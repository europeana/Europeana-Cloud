package eu.europeana.cloud.service.dps.examples.toplologies;


import eu.europeana.cloud.service.dps.examples.StaticDpsTaskSpout;
import eu.europeana.cloud.service.dps.examples.toplologies.builder.SimpleStaticTopologyBuilder;
import eu.europeana.cloud.service.dps.examples.util.DpsTaskUtil;
import eu.europeana.cloud.service.dps.examples.util.TopologyConfigBuilder;
import eu.europeana.cloud.service.dps.storm.utils.TopologyHelper;
import eu.europeana.cloud.service.dps.storm.xslt.XsltBolt;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.utils.Utils;


import static eu.europeana.cloud.service.dps.examples.toplologies.constants.TopologyConstants.*;


/**
 * Example for XSLTTopology to read Datasets:
 * <p/>
 * - Creates a DpsTask using {@link StaticDpsTaskSpout}
 * <p/>
 * - Reads a DataSet/DataSets or part of them using Revisions from eCloud ; Reads Files inside those DataSets , transform those files using XSLT_Transform file;
 * - Writes those transformed Files to eCloud ; Assigns them to outputRevision in case specified and assigns them to output dataSets in case specified!.
 */
public class StaticXsltTopology {
    public static final String TOPOLOGY_NAME = "xslt_topology";

    public static void main(String[] args) {
        StaticDpsTaskSpout staticDpsTaskSpout = new StaticDpsTaskSpout(DpsTaskUtil.generateDpsTaskForXSLT(), TOPOLOGY_NAME);
        StormTopology stormTopology = SimpleStaticTopologyBuilder.buildTopology(staticDpsTaskSpout, new XsltBolt(), TopologyHelper.XSLT_BOLT, MCS_URL);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, TopologyConfigBuilder.buildConfig(), stormTopology);
        Utils.sleep(60000);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
    }
}
