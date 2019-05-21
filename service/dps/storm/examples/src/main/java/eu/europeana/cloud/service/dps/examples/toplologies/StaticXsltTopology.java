package eu.europeana.cloud.service.dps.examples.toplologies;


import eu.europeana.cloud.service.dps.examples.StaticDpsTaskSpout;
import eu.europeana.cloud.service.dps.examples.toplologies.builder.SimpleStaticTopologyBuilder;
import eu.europeana.cloud.service.dps.examples.util.TopologyConfigBuilder;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.MCSReaderSpout;
import eu.europeana.cloud.service.dps.storm.topologies.xslt.bolt.XsltBolt;
import eu.europeana.cloud.service.dps.storm.utils.TopologyHelper;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
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

        SpoutConfig kafkaConfig = new SpoutConfig(new ZkHosts(ZOOKEEPER_HOST), TOPOLOGY_NAME, "", "storm");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaConfig.ignoreZkOffsets = true;
        kafkaConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        MCSReaderSpout kafkaSpout = new MCSReaderSpout(kafkaConfig, CASSANDRA_HOSTS, Integer.parseInt(CASSANDRA_PORT), CASSANDRA_KEYSPACE_NAME, CASSANDRA_USERNAME, CASSANDRA_SECRET_TOKEN,MCS_URL);

        StormTopology stormTopology = SimpleStaticTopologyBuilder.buildTopology(kafkaSpout, new XsltBolt(), TopologyHelper.XSLT_BOLT, MCS_URL);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, TopologyConfigBuilder.buildConfig(), stormTopology);
        Utils.sleep(6000000);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
    }
}
