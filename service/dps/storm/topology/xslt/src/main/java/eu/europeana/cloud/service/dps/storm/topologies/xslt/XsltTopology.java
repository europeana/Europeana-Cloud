package eu.europeana.cloud.service.dps.storm.topologies.xslt;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import eu.europeana.cloud.service.dps.storm.io.ReadFileBolt;
import eu.europeana.cloud.service.dps.storm.io.WriteRecordBolt;
import eu.europeana.cloud.service.dps.storm.kafka.KafkaParseTaskBolt;
import eu.europeana.cloud.service.dps.storm.xslt.XsltBolt;

public class XsltTopology {

    private static String ecloudMcsAddress = "http://146.48.82.158:8080/ecloud-service-mcs-rest-0.3-SNAPSHOT";
    private static String username = "Cristiano";
    private static String password = "Ronaldo";

    public static final Logger LOGGER = LoggerFactory.getLogger(XsltTopology.class);

    private final BrokerHosts brokerHosts;

    public XsltTopology(String kafkaZookeeper) {
        brokerHosts = new ZkHosts(kafkaZookeeper);
    }

    public StormTopology buildTopology() {
        ReadFileBolt retrieveFileBolt = new ReadFileBolt(ecloudMcsAddress, username, password);
        WriteRecordBolt writeRecordBolt = new WriteRecordBolt(ecloudMcsAddress, username, password);

        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, "franco_maria_topic", "", "storm");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("kafkaReader", new KafkaSpout(kafkaConfig), 10);
        builder.setBolt("parseKafkaInput", new KafkaParseTaskBolt()).shuffleGrouping("kafkaReader");
        builder.setBolt("retrieveFileBolt", retrieveFileBolt).shuffleGrouping("parseKafkaInput");
        builder.setBolt("xsltTransformationBolt", new XsltBolt()).shuffleGrouping("retrieveFileBolt");
        builder.setBolt("writeRecordBolt", writeRecordBolt).shuffleGrouping("xsltTransformationBolt");
        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {
        // String kafkaZk = args[0];
        String kafkaZk = "ecloud.eanadev.org:2181";
        XsltTopology kafkaSpoutTestTopology = new XsltTopology(kafkaZk);
        Config config = new Config();
        config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 2000);

        StormTopology stormTopology = kafkaSpoutTestTopology.buildTopology();
        if (args != null && args.length > 1) {
            String name = args[0];
            String dockerIp = args[1];
            config.setNumWorkers(2);
            config.setMaxTaskParallelism(5);
            config.put(Config.NIMBUS_HOST, dockerIp);
            config.put(Config.NIMBUS_THRIFT_PORT, 6627);
            config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
            config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(dockerIp));
            StormSubmitter.submitTopology(name, config, stormTopology);
        } else {
            config.setNumWorkers(2);
            config.setMaxTaskParallelism(2);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("kafka", config, stormTopology);
        }
    }
}
