package eu.europeana.cloud.service.dps.examples.toplologies;

import eu.europeana.cloud.normalization.bolts.NormalizationBolt;
import eu.europeana.cloud.service.dps.examples.toplologies.builder.SimpleStaticTopologyBuilder;
import eu.europeana.cloud.service.dps.examples.util.TopologyConfigBuilder;
import eu.europeana.cloud.service.dps.storm.spout.MCSReaderSpout;
import eu.europeana.cloud.service.dps.storm.utils.TopologyHelper;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.utils.Utils;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyDefaultsConstants.*;

/**
 * Created by Tarek on 10/2/2017.
 */
public class StaticNormalizationTopology {
    public static final String TOPOLOGY_NAME = "normalization_topology";
    public static void main(String[] args) {

/*
        SpoutConfig kafkaConfig = new SpoutConfig(new ZkHosts(ZOOKEEPER_HOST), TOPOLOGY_NAME, "", "storm");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaConfig.ignoreZkOffsets = true;
        kafkaConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
*/

        KafkaSpoutConfig kafkaConfig = KafkaSpoutConfig
                .builder( DEFAULT_KAFKA_HOST, TOPOLOGY_NAME)
                .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_MOST_ONCE)
                .setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_EARLIEST)
                .build();

        MCSReaderSpout kafkaSpout = new MCSReaderSpout(kafkaConfig,
                DEFAULT_CASSANDRA_HOSTS,
                Integer.parseInt(DEFAULT_CASSANDRA_PORT),
                DEFAULT_CASSANDRA_KEYSPACE_NAME,
                DEFAULT_CASSANDRA_USERNAME,
                DEFAULT_CASSANDRA_SECRET_TOKEN,
                DEFAULT_MCS_URL
        );

        StormTopology stormTopology = SimpleStaticTopologyBuilder.buildTopology(kafkaSpout, new NormalizationBolt(),
                TopologyHelper.NORMALIZATION_BOLT, DEFAULT_MCS_URL);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, TopologyConfigBuilder.buildConfig(), stormTopology);
        Utils.sleep(60000);
        cluster.killTopology(TOPOLOGY_NAME);
        cluster.shutdown();
    }
}
