package eu.europeana.cloud.service.dps.storm.textstripping;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import eu.europeana.cloud.service.dps.storm.io.RetrieveDatasetBolt;
import java.util.UUID;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

/**
 *
 * @author la4227 <lucas.anastasiou@open.ac.uk>
 */
public class TextStrippingTopology {

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        // spouts and bolts definitions
        //
        //spout that consumes from kafka
        BrokerHosts hosts = new ZkHosts(TextStrippingConstants.ZOOKEEPER_CONNECTION_STRING);
        SpoutConfig spoutConfig = new SpoutConfig(hosts, TextStrippingConstants.KAFKA_TOPIC, "/" + TextStrippingConstants.KAFKA_TOPIC, UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
        KafkaParseTextStrippingTaskBolt bolt = new KafkaParseTextStrippingTaskBolt();
        RetrieveDatasetBolt retrievedatasetBolt = new RetrieveDatasetBolt(TextStrippingConstants.ZOOKEEPER_CONNECTION_STRING, TextStrippingConstants.MCS_URL, TextStrippingConstants.USERNAME, TextStrippingConstants.PASSWORD);
        DummyBolt dummy = new DummyBolt();

        // connect spouts and bolts to a topology
        //
        builder.setSpout("kafkaSpout", kafkaSpout, 1);
        builder.setBolt("parseDpsTask", bolt, 1).shuffleGrouping("kafkaSpout");
        builder.setBolt("retrievedatasetBolt", retrievedatasetBolt, 1).shuffleGrouping("parseDpsTask");
        builder.setBolt("dummyBolt", dummy, 1).shuffleGrouping("retrievedatasetBolt");

        Config conf = new Config();
        conf.put(Config.TOPOLOGY_DEBUG, false);

        if (args != null && args.length > 0) {

            conf.setNumWorkers(3);

            StormSubmitter.submitTopologyWithProgressBar(TextStrippingConstants.TOPOLOGY_NAME, conf,
                    builder.createTopology());
        } else {
            // local deploy - only devel
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(TextStrippingConstants.TOPOLOGY_NAME, conf, builder.createTopology());
            Utils.sleep(6000000);
            cluster.killTopology("test");
            cluster.shutdown();
        }

    }
}
