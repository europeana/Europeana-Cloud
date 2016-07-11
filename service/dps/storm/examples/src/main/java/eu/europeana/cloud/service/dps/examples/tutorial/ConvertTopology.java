package eu.europeana.cloud.service.dps.examples.tutorial;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

import static eu.europeana.cloud.service.dps.examples.tutorial.TopologyConstants.*;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.NotificationBolt;
import eu.europeana.cloud.service.dps.storm.ParseTaskBolt;
import eu.europeana.cloud.service.dps.storm.io.ReadFileBolt;
import eu.europeana.cloud.service.dps.storm.io.StoreFileAsRepresentationBolt;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import java.util.HashMap;
import java.util.Map;

/**
 * @author lucasanastasiou
 */
public class ConvertTopology {
    private final static String datasetStream = "DATASET_URLS";
    private final static String fileStream = "FILE_URLS";

    public static void main(String args[]) {
        TopologyBuilder builder = new TopologyBuilder();

        Map<String, String> routingRules = new HashMap<>();
        routingRules.put(PluginParameterKeys.FILE_URLS, datasetStream);
        routingRules.put(PluginParameterKeys.DATASET_URLS, fileStream);

        // entry spout
        BrokerHosts brokerHosts = new ZkHosts(ZOOKEEPER_LOCATION);
        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, KAFKA_TOPIC, ZK_ROOT, ID);
        builder.setSpout("KafkaSpout", new KafkaSpout(spoutConf), 1);

        //bolt 1
        builder.setBolt("ParseDpsTask", new ParseTaskBolt(routingRules), 1)
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


        //notification bolt
        builder.setBolt("NotificationBolt", new NotificationBolt(CASSANDRA_HOSTS, CASSANDRA_PORT, CASSANDRA_KEYSPACE_NAME, CASSANDRA_USERNAME, CASSANDRA_PASSWORD), 1)
                .shuffleGrouping("ParseDpsTask")
                .shuffleGrouping("RetrieveFile")
                .shuffleGrouping("ConvertBolt")
                .shuffleGrouping("StoreBolt");

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
