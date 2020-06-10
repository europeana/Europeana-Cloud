package eu.europeana.cloud.service.dps.examples.toplologies;


import eu.europeana.cloud.service.dps.examples.toplologies.builder.SimpleStaticOAITopologyBuilder;
import eu.europeana.cloud.service.dps.examples.util.TopologyConfigBuilder;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.spout.OAISpout;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.utils.Utils;

import static eu.europeana.cloud.service.dps.examples.toplologies.constants.TopologyConstants.*;
import static eu.europeana.cloud.service.dps.storm.utils.TopologiesNames.OAI_TOPOLOGY;

/**
 * Created by Tarek on 10/2/2017.
 */
public class StaticOAITopology {

    public static void main(String[] args) {

/*
        SpoutConfig kafkaConfig = new SpoutConfig(new ZkHosts(ZOOKEEPER_HOST), OAI_TOPOLOGY, "", "storm");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaConfig.ignoreZkOffsets = true;
        kafkaConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
*/

        KafkaSpoutConfig kafkaConfig = KafkaSpoutConfig
                .builder( KAFKA_HOST, OAI_TOPOLOGY)
                .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_MOST_ONCE)
                .setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_EARLIEST)
                .build();

        OAISpout kafkaSpout = new OAISpout(kafkaConfig, CASSANDRA_HOSTS, Integer.parseInt(CASSANDRA_PORT), CASSANDRA_KEYSPACE_NAME, CASSANDRA_USERNAME, CASSANDRA_SECRET_TOKEN);
        StormTopology stormTopology = SimpleStaticOAITopologyBuilder.buildTopology(kafkaSpout, UIS_URL, MCS_URL);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(OAI_TOPOLOGY, TopologyConfigBuilder.buildConfig(), stormTopology);
        Utils.sleep(1000*60*1000); //1000 minutes
        cluster.killTopology(OAI_TOPOLOGY);
        cluster.shutdown();


    }

}
