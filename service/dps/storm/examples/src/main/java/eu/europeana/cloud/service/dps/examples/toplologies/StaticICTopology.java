package eu.europeana.cloud.service.dps.examples.toplologies;

import eu.europeana.cloud.http.spout.HttpKafkaSpout;
import eu.europeana.cloud.service.dps.examples.StaticDpsTaskSpout;
import eu.europeana.cloud.service.dps.examples.toplologies.builder.SimpleStaticTopologyBuilder;
import eu.europeana.cloud.service.dps.examples.toplologies.constants.TopologyConstants;
import eu.europeana.cloud.service.dps.examples.util.DpsTaskUtil;
import eu.europeana.cloud.service.dps.examples.util.TopologyConfigBuilder;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.MCSReaderSpout;
import eu.europeana.cloud.service.dps.storm.topologies.ic.topology.bolt.IcBolt;
import eu.europeana.cloud.service.dps.storm.utils.TopologyHelper;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.utils.Utils;

import static eu.europeana.cloud.service.dps.examples.toplologies.constants.TopologyConstants.*;
import static eu.europeana.cloud.service.dps.examples.toplologies.constants.TopologyConstants.CASSANDRA_SECRET_TOKEN;
import static eu.europeana.cloud.service.dps.examples.toplologies.constants.TopologyConstants.CASSANDRA_USERNAME;

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

    public static void main(String[] args) {

        SpoutConfig kafkaConfig = new SpoutConfig(new ZkHosts(ZOOKEEPER_HOST), TOPOLOGY_NAME, "", "storm");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaConfig.ignoreZkOffsets = true;
        kafkaConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        MCSReaderSpout kafkaSpout = new MCSReaderSpout(kafkaConfig, CASSANDRA_HOSTS, Integer.parseInt(CASSANDRA_PORT), CASSANDRA_KEYSPACE_NAME, CASSANDRA_USERNAME, CASSANDRA_SECRET_TOKEN,MCS_URL);

        StormTopology stormTopology = SimpleStaticTopologyBuilder.buildTopology(kafkaSpout, new IcBolt(), TopologyHelper.IC_BOLT, TopologyConstants.MCS_URL);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, TopologyConfigBuilder.buildConfig(), stormTopology);
        Utils.sleep(60000000);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();

    }
}



