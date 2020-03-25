package eu.europeana.cloud.service.dps.examples.toplologies;

import eu.europeana.cloud.http.spout.HttpKafkaSpout;
import eu.europeana.cloud.service.dps.examples.toplologies.builder.SimpleStaticHTTPTopologyBuilder;
import eu.europeana.cloud.service.dps.examples.util.TopologyConfigBuilder;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.utils.Utils;

import static eu.europeana.cloud.service.dps.examples.toplologies.constants.TopologyConstants.*;

/**
 * Created by Tarek on 10/2/2017.
 */
public class StaticHttpTopology {
    public static final String TOPOLOGY_NAME = "http_topology";

    public static void main(String[] args) {
/*
        SpoutConfig kafkaConfig = new SpoutConfig(new ZkHosts(ZOOKEEPER_HOST), TOPOLOGY_NAME, "", "storm");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaConfig.ignoreZkOffsets = true;
        kafkaConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
*/

        KafkaSpoutConfig kafkaConfig = KafkaSpoutConfig
                .builder( KAFKA_HOST, TOPOLOGY_NAME)
                .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_MOST_ONCE)
                .setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_EARLIEST)
                .build();

        HttpKafkaSpout kafkaSpout = new HttpKafkaSpout(kafkaConfig, CASSANDRA_HOSTS, Integer.parseInt(CASSANDRA_PORT), CASSANDRA_KEYSPACE_NAME, CASSANDRA_USERNAME, CASSANDRA_SECRET_TOKEN);
        StormTopology stormTopology = SimpleStaticHTTPTopologyBuilder.buildTopology(kafkaSpout, UIS_URL, MCS_URL);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, TopologyConfigBuilder.buildConfig(), stormTopology);
        Utils.sleep(60000000);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();

    }
}
