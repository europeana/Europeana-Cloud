package eu.europeana.cloud.service.dps.examples.kafka;

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
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class KafkaTopology {
    public static final Logger LOG = LoggerFactory.getLogger(KafkaTopology.class);

    public static class StupidBolt extends BaseBasicBolt {
        private static final long serialVersionUID = 1L;

		@Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            LOG.info(tuple.toString());
        }
    }

    private final BrokerHosts brokerHosts;

    public KafkaTopology(String kafkaZookeeper) {
        brokerHosts = new ZkHosts(kafkaZookeeper);
    }

    public StormTopology buildTopology() {
        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, "franco_maria_topic", "", "storm");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("eCloud", new KafkaSpout(kafkaConfig), 10);
        builder.setBolt("eCloudOutput", new StupidBolt()).shuffleGrouping("eCloud");
        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {

        //String kafkaZk = args[0];
    	String kafkaZk = "ecloud.eanadev.org:2181";
        KafkaTopology kafkaSpoutTestTopology = new KafkaTopology(kafkaZk);
        Config config = new Config();
        config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 2000);

        StormTopology stormTopology = kafkaSpoutTestTopology.buildTopology();
        if (args != null && args.length > 1) {
            String name = args[1];
            String dockerIp = args[2];
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
