package eu.europeana.cloud.service.dps.examples.toplologies;


import eu.europeana.cloud.service.dps.examples.toplologies.builder.SimpleStaticOAITopologyBuilder;
import eu.europeana.cloud.service.dps.examples.util.TopologyConfigBuilder;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.spout.OAISpout;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.utils.Utils;


import static eu.europeana.cloud.service.dps.examples.toplologies.constants.TopologyConstants.*;


/**
 * Created by Tarek on 10/2/2017.
 */
public class StaticOAITopology {
    public static final String TOPOLOGY_NAME = "oai_topology";

    public static void main(String[] args) {
        SpoutConfig kafkaConfig = new SpoutConfig(new ZkHosts(ZOOKEEPER_HOST), TOPOLOGY_NAME, "", "storm");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaConfig.ignoreZkOffsets = true;
        kafkaConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        OAISpout kafkaSpout = new OAISpout(kafkaConfig, CASSANDRA_HOSTS, Integer.parseInt(CASSANDRA_PORT), CASSANDRA_KEYSPACE_NAME, CASSANDRA_USERNAME, CASSANDRA_SECRET_TOKEN,MCS_URL);
        StormTopology stormTopology = SimpleStaticOAITopologyBuilder.buildTopology(kafkaSpout, UIS_URL, MCS_URL);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, TopologyConfigBuilder.buildConfig(), stormTopology);
        Utils.sleep(60000000);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();

    }

}
