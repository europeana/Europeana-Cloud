package eu.europeana.cloud.service.dps.storm.topologies.text;

import eu.europeana.cloud.service.dps.storm.ParseTaskBolt;
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
import eu.europeana.cloud.service.dps.storm.KafkaProducerBolt;
import eu.europeana.cloud.service.dps.storm.ProgressBolt;
import eu.europeana.cloud.service.dps.storm.io.ReadDatasetBolt;
import eu.europeana.cloud.service.dps.storm.io.ReadFileBolt;
import eu.europeana.cloud.service.dps.storm.io.StoreFileAsNewRepresentationBolt;
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
public class TextStrippingTopology 
{
    private final String datasetStream = "ReadDataset";
    private final String fileStream = "ReadFile";
    private final String storeStream = "StoreStream";
    private final String informStream = "InformStream";
    
    private final String zkProgressAddress = TextStrippingConstants.PROGRESS_ZOOKEEPER;
    private final String ecloudMcsAddress = TextStrippingConstants.MCS_URL;
    private final String username = TextStrippingConstants.USERNAME;
    private final String password = TextStrippingConstants.PASSWORD;
    
    private static final Logger LOGGER = LoggerFactory.getLogger(TextStrippingTopology.class);
    
    private final BrokerHosts brokerHosts;

    /**
     * 
     * @param brokerZkStr zookeeper connection string (e.g. localhost:2181)
     */
    public TextStrippingTopology(String brokerZkStr) 
    {
        brokerHosts = new ZkHosts(brokerZkStr);
    }
    
    private StormTopology buildTopology() 
    {
        Map<String, String> routingRules = new HashMap<>();
        routingRules.put(PluginParameterKeys.NEW_DATASET_MESSAGE, datasetStream);
        routingRules.put(PluginParameterKeys.NEW_FILE_MESSAGE, fileStream);
        
        Map<String, String> prerequisites = new HashMap<>();
        prerequisites.put(PluginParameterKeys.EXTRACT_TEXT, "True");
        
        Map<String, String> outputParameters = new HashMap<>();
        outputParameters.put(PluginParameterKeys.INDEX_DATA, null);
        outputParameters.put(PluginParameterKeys.CLOUD_ID, null);
        outputParameters.put(PluginParameterKeys.REPRESENTATION_NAME, null);
        outputParameters.put(PluginParameterKeys.REPRESENTATION_VERSION, null);
        outputParameters.put(PluginParameterKeys.FILE_NAME, null);
        outputParameters.put(PluginParameterKeys.ORIGINAL_FILE_URL, null);
        outputParameters.put(PluginParameterKeys.FILE_METADATA, null);
             
        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, 
                TextStrippingConstants.KAFKA_INPUT_TOPIC, 
                TextStrippingConstants.ZOOKEEPER_ROOT, UUID.randomUUID().toString());
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());               
                
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("kafkaSpout", new KafkaSpout(kafkaConfig), TextStrippingConstants.KAFKA_SPOUT_PARALLEL);
        
        builder.setBolt("parseDpsTask", new ParseTaskBolt(routingRules, prerequisites), TextStrippingConstants.PARSE_TASKS_BOLT_PARALLEL)
                .shuffleGrouping("kafkaSpout");
        
        builder.setBolt("retrieveDataset", new ReadDatasetBolt(zkProgressAddress, ecloudMcsAddress, username, password), 
                            TextStrippingConstants.DATASET_BOLT_PARALLEL)
                .shuffleGrouping("parseDpsTask", datasetStream);
        
        builder.setBolt("retrieveFile", new ReadFileBolt(zkProgressAddress, ecloudMcsAddress, username, password), 
                            TextStrippingConstants.FILE_BOLT_PARALLEL)
                .shuffleGrouping("parseDpsTask", fileStream);
        
        builder.setBolt("extractText", new ExtractTextBolt(storeStream, informStream), TextStrippingConstants.EXTRACT_BOLT_PARALLEL)
                .shuffleGrouping("retrieveDataset")
                .shuffleGrouping("retrieveFile");
        
        builder.setBolt("storeNewRepresentation", new StoreFileAsNewRepresentationBolt(zkProgressAddress, ecloudMcsAddress, username, password), 
                            TextStrippingConstants.STORE_BOLT_PARALLEL)
                .shuffleGrouping("extractText", storeStream);
        
        builder.setBolt("InformBolt", new KafkaProducerBolt(TextStrippingConstants.KAFKA_OUTPUT_BROKER, TextStrippingConstants.KAFKA_OUTPUT_TOPIC,
                            PluginParameterKeys.NEW_EXTRACTED_DATA_MESSAGE, outputParameters), TextStrippingConstants.INFORM_BOLT_PARALLEL)
                .shuffleGrouping("extractText", informStream)
                .shuffleGrouping("storeNewRepresentation");
        
        builder.setBolt("progress", new ProgressBolt(zkProgressAddress), TextStrippingConstants.PROGRESS_BOLT_PARALLEL)
                .shuffleGrouping("InformBolt");
        
        return builder.createTopology();
    }
    
    /**
     * @param args the command line arguments
     * @throws backtype.storm.generated.AlreadyAliveException
     * @throws backtype.storm.generated.InvalidTopologyException
     */
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException 
    {
        String kafkaZk = TextStrippingConstants.INPUT_ZOOKEEPER;//args[0];
        TextStrippingTopology textStrippingTopology = new TextStrippingTopology(kafkaZk);
        Config config = new Config();
        config.setDebug(true);
        //config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 2000);

        Map<String, String> kafkaMetricsConfig = new HashMap<>();
        kafkaMetricsConfig.put(KafkaMetricsConsumer.KAFKA_BROKER_KEY, TextStrippingConstants.KAFKA_METRICS_BROKER);
        kafkaMetricsConfig.put(KafkaMetricsConsumer.KAFKA_TOPIC_KEY, TextStrippingConstants.KAFKA_METRICS_TOPIC);
        config.registerMetricsConsumer(KafkaMetricsConsumer.class, kafkaMetricsConfig, TextStrippingConstants.METRICS_CONSUMER_PARALLEL);
        
        StormTopology stormTopology = textStrippingTopology.buildTopology();

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
            cluster.submitTopology("TextStrippingTopology", config, stormTopology);
            Utils.sleep(6000000);
            cluster.killTopology("TextStrippingTopology");
            cluster.shutdown();
        }
    }
}
