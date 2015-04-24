package eu.europeana.cloud.service.dps.storm.topologies.indexer;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.KafkaMetricsConsumer;
import eu.europeana.cloud.service.dps.storm.ParseTaskBolt;
import eu.europeana.cloud.service.dps.storm.ProgressBolt;
import eu.europeana.cloud.service.dps.storm.io.ReadFileBolt;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class IndexerTopology 
{
    private final String zkProgressAddress = IndexerConstants.PROGRESS_ZOOKEEPER;
    private final String ecloudMcsAddress = IndexerConstants.MCS_URL;
    private final String username = IndexerConstants.USERNAME;
    private final String password = IndexerConstants.PASSWORD;
    
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexerTopology.class);
    
    private final BrokerHosts brokerHosts;
    
    public IndexerTopology(String brokerZkStr) 
    {
        brokerHosts = new ZkHosts(brokerZkStr);
    }
    
    private StormTopology buildTopology()
    {
        Map<String, String> routingRules = new HashMap<>();
        routingRules.put(PluginParameterKeys.INDEX_FILE_MESSAGE, "IndexFile");
        
        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, 
                IndexerConstants.KAFKA_TOPIC, 
                IndexerConstants.ZOOKEEPER_ROOT, UUID.randomUUID().toString());
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());               
                
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("kafkaSpout", new KafkaSpout(kafkaConfig), IndexerConstants.KAFKA_SPOUT_PARALLEL);
        
        builder.setBolt("parseDpsTask", new ParseTaskBolt(routingRules), IndexerConstants.PARSE_TASKS_BOLT_PARALLEL)
                .shuffleGrouping("kafkaSpout");
        
        builder.setBolt("retrieveFile", new ReadFileBolt(zkProgressAddress, ecloudMcsAddress, username, password), 
                            IndexerConstants.FILE_BOLT_PARALLEL)
                .shuffleGrouping("parseDpsTask", "IndexFile");
        
        builder.setBolt("indexFile", new IndexBolt(IndexerConstants.ELASTICSEARCH_ADDRESSES), IndexerConstants.INDEX_BOLT_PARALLEL)
                .shuffleGrouping("retrieveFile");
        
        builder.setBolt("progress", new ProgressBolt(zkProgressAddress), IndexerConstants.PROGRESS_BOLT_PARALLEL)
                .shuffleGrouping("indexFile");     

        return builder.createTopology();
    }
    
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException 
    {
        String kafkaZk = "192.168.47.129:2181";//args[0];
        IndexerTopology indexerTopology = new IndexerTopology(kafkaZk);
        Config config = new Config();
        config.setDebug(true);
        //config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 2000);

        Map<String, String> kafkaMetricsConfig = new HashMap<>();
        kafkaMetricsConfig.put(KafkaMetricsConsumer.KAFKA_BROKER_KEY, IndexerConstants.KAFKA_METRICS_BROKER);
        kafkaMetricsConfig.put(KafkaMetricsConsumer.KAFKA_TOPIC_KEY, IndexerConstants.KAFKA_METRICS_TOPIC);
        config.registerMetricsConsumer(KafkaMetricsConsumer.class, kafkaMetricsConfig, IndexerConstants.METRICS_CONSUMER_PARALLEL);
        
        StormTopology stormTopology = indexerTopology.buildTopology();

        if (args != null && args.length > 1) 
        {
            String dockerIp = args[1];
            String name = args[2];
            config.setNumWorkers(1);
            config.setMaxTaskParallelism(1);
            config.put(Config.NIMBUS_THRIFT_PORT, 6627);
            config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
            config.put(Config.NIMBUS_HOST, dockerIp);
            config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(kafkaZk));
            StormSubmitter.submitTopology(name, config, stormTopology);
        } 
        else 
        {
            config.setNumWorkers(1);
            config.setMaxTaskParallelism(1);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("IndexerTopology", config, stormTopology);
            Utils.sleep(6000000);
            cluster.killTopology("IndexerTopology");
            cluster.shutdown();
        }
    }
}
