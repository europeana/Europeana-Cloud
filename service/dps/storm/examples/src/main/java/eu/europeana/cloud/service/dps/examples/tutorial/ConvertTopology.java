package eu.europeana.cloud.service.dps.examples.tutorial;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import static eu.europeana.cloud.service.dps.examples.tutorial.TopologyConstants.*;
import eu.europeana.cloud.service.dps.storm.EndBolt;
import eu.europeana.cloud.service.dps.storm.NotificationBolt;
import eu.europeana.cloud.service.dps.storm.ParseTaskBolt;
import eu.europeana.cloud.service.dps.storm.io.ReadFileBolt;
import eu.europeana.cloud.service.dps.storm.io.StoreFileAsRepresentationBolt;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

/**
 *
 * @author lucasanastasiou
 */
public class ConvertTopology {

    public static void main(String args[]) {
        TopologyBuilder builder = new TopologyBuilder();
        
        // entry spout
        BrokerHosts brokerHosts = new ZkHosts(ZOOKEEPER_LOCATION);
        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, KAFKA_TOPIC, ZK_ROOT, ID);
        builder.setSpout("KafkaSpout", new KafkaSpout(spoutConf), 1);

        //bolt 1
        builder.setBolt("ParseDpsTask", new ParseTaskBolt(), 1)
                .shuffleGrouping("KafkaSpout");

        //bolt 2
        builder.setBolt("RetrieveFile", new ReadFileBolt(ECLOUD_MCS_ADDRESS), 1)
                .shuffleGrouping("ParseDpsTask");

        //bolt 3
        builder.setBolt("ConvertBolt", new ConvertBolt(), 1)
                .shuffleGrouping("RetrieveFile");

        //bolt 4
        builder.setBolt("StoreBolt", new StoreFileAsRepresentationBolt(ECLOUD_MCS_ADDRESS, ECLOUD_MCS_USERNAME, ECLOUD_MCS_PASSWORD), 1)
                .shuffleGrouping("ConvertBolt", "stream-to-next-bolt");

        //bolt5
        builder.setBolt("EndBolt", new EndBolt(), 1).shuffleGrouping("StoreBolt");

        //notification bolt
        builder.setBolt("NotificationBolt", new NotificationBolt(CASSANDRA_HOSTS, CASSANDRA_PORT, CASSANDRA_KEYSPACE_NAME, CASSANDRA_USERNAME, CASSANDRA_PASSWORD), 1)
                .shuffleGrouping("ParseDpsTask")
                .shuffleGrouping("RetrieveFile")
                .shuffleGrouping("ConvertBolt")
                .shuffleGrouping("StoreBolt")
                .shuffleGrouping("EndBolt");

// run in local mode for debugging...
        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(2);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
        Utils.sleep(10000);
        cluster.killTopology("test");
        cluster.shutdown();

    }
}
