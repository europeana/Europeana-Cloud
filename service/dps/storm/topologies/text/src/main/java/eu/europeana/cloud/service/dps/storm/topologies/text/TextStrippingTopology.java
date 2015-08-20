package eu.europeana.cloud.service.dps.storm.topologies.text;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.testing.FeederSpout;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.EndBolt;
import eu.europeana.cloud.service.dps.storm.NotificationBolt;
import eu.europeana.cloud.service.dps.storm.ParseTaskBolt;
import eu.europeana.cloud.service.dps.storm.io.ReadDatasetBolt;
import eu.europeana.cloud.service.dps.storm.io.ReadFileBolt;
import eu.europeana.cloud.service.dps.storm.io.StoreFileAsRepresentationBolt;
import eu.europeana.cloud.service.dps.storm.kafka.KafkaMetricsConsumer;

/**
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class TextStrippingTopology 
{
    public enum SpoutType
    {
        KAFKA,
        FEEDER
    }
    
    private final SpoutType spoutType;
    private final String topologyName;
    
    private final String datasetStream = "ReadDataset";
    private final String fileStream = "ReadFile";
    
    private final String ecloudMcsAddress = TextStrippingConstants.MCS_URL;
    private final String username = TextStrippingConstants.USERNAME;
    private final String password = TextStrippingConstants.PASSWORD;
    
    /**
     * 
     * @param spoutType
     */
    public TextStrippingTopology(SpoutType spoutType) 
    {
        this(spoutType, "Text stripping topology");
    }
    
    /**
     * 
     * @param spoutType
     * @param topologyName
     */
    public TextStrippingTopology(SpoutType spoutType, String topologyName) 
    {
        this.spoutType = spoutType;
        this.topologyName = topologyName;
    }
    
    protected StormTopology buildTopology() 
    {
        Map<String, String> routingRules = new HashMap<>();
        routingRules.put(PluginParameterKeys.NEW_DATASET_MESSAGE, datasetStream);
        routingRules.put(PluginParameterKeys.NEW_FILE_MESSAGE, fileStream);
        
        Map<String, String> prerequisites = new HashMap<>();
        prerequisites.put(PluginParameterKeys.EXTRACT_TEXT, "True");
        
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("KafkaSpout", getSpout(), TextStrippingConstants.KAFKA_SPOUT_PARALLEL);
        
        builder.setBolt("ParseDpsTask", new ParseTaskBolt(routingRules, prerequisites), TextStrippingConstants.PARSE_TASKS_BOLT_PARALLEL)
                .shuffleGrouping("KafkaSpout");
        
        builder.setBolt("RetrieveDataset", new ReadDatasetBolt(ecloudMcsAddress, username, password), 
                            TextStrippingConstants.DATASET_BOLT_PARALLEL)
                .shuffleGrouping("ParseDpsTask", datasetStream);
        
        builder.setBolt("RetrieveFile", new ReadFileBolt(ecloudMcsAddress, username, password), 
                            TextStrippingConstants.FILE_BOLT_PARALLEL)
                .shuffleGrouping("ParseDpsTask", fileStream);
        
        builder.setBolt("ExtractText", new ExtractTextBolt(), TextStrippingConstants.EXTRACT_BOLT_PARALLEL)
                .shuffleGrouping("RetrieveDataset")
                .shuffleGrouping("RetrieveFile");
        
        builder.setBolt("StoreNewRepresentation", new StoreFileAsRepresentationBolt(ecloudMcsAddress, username, password), 
                            TextStrippingConstants.STORE_BOLT_PARALLEL)
                .shuffleGrouping("ExtractText");

        builder.setBolt("EndBolt", new EndBolt(), TextStrippingConstants.END_BOLT_PARALLEL)
                .shuffleGrouping("StoreNewRepresentation");
        
        builder.setBolt("NotificationBolt", new NotificationBolt(topologyName, TextStrippingConstants.CASSANDRA_HOSTS, 
                            TextStrippingConstants.CASSANDRA_PORT, TextStrippingConstants.CASSANDRA_KEYSPACE_NAME,
                            TextStrippingConstants.CASSANDRA_USERNAME, TextStrippingConstants.CASSANDRA_PASSWORD), 
                            TextStrippingConstants.NOTIFICATION_BOLT_PARALLEL)
                .shuffleGrouping("ParseDpsTask", AbstractDpsBolt.NOTIFICATION_STREAM_NAME)
                .shuffleGrouping("RetrieveDataset", AbstractDpsBolt.NOTIFICATION_STREAM_NAME)
                .shuffleGrouping("RetrieveFile", AbstractDpsBolt.NOTIFICATION_STREAM_NAME)
                .shuffleGrouping("ExtractText", AbstractDpsBolt.NOTIFICATION_STREAM_NAME)
                .shuffleGrouping("StoreNewRepresentation", AbstractDpsBolt.NOTIFICATION_STREAM_NAME)
                .shuffleGrouping("EndBolt", AbstractDpsBolt.NOTIFICATION_STREAM_NAME);
        
        return builder.createTopology();
    }
    
    private IRichSpout getSpout()
    {    
        switch(spoutType)
        {
            case FEEDER:
                return new FeederSpout(new StringScheme().getOutputFields());
            case KAFKA:
            default:
                SpoutConfig kafkaConfig = new SpoutConfig(
                    new ZkHosts(TextStrippingConstants.INPUT_ZOOKEEPER), 
                    TextStrippingConstants.KAFKA_INPUT_TOPIC, 
                    TextStrippingConstants.ZOOKEEPER_ROOT, UUID.randomUUID().toString());
                kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
                return new KafkaSpout(kafkaConfig);
        }
    }
    
    /**
     * @param args the command line arguments
     * <ol>
     * <li>topology name (e.g. index_topology)</li>
     * <li>number of workers (e.g. 1)</li>
     * <li>max task parallelism (e.g. 1)</li>
     * <!--
     * <li>zookeeper servers (e.g. localhost;another.server.com) - STORM_ZOOKEEPER_SERVERS</li>
     * <li>zookeeper port (e.g. 2181) - STORM_ZOOKEEPER_PORT</li>
     * <li>nimbus host (e.g. localhost) - NIMBUS_HOST</li>
     * <li>nimbus port (e.g. 6627) - NIMBUS_THRIFT_PORT</li>
     * -->
     * <li>JVM parameters (e.g. "-Dhttp.proxyHost=xxx -Dhttp.proxyPort=xx") - TOPOLOGY_WORKER_CHILDOPTS</li>
     * </ol>
     * 
     * @throws backtype.storm.generated.AlreadyAliveException
     * @throws backtype.storm.generated.InvalidTopologyException
     * @throws backtype.storm.generated.AuthorizationException
     */
    public static void main(String[] args) 
            throws AlreadyAliveException, InvalidTopologyException, AuthorizationException 
    {
        TextStrippingTopology textStrippingTopology;
        if(args != null && args.length > 0)
        {
            textStrippingTopology = new TextStrippingTopology(SpoutType.KAFKA, args[0]);
        }
        else
        {
            textStrippingTopology = new TextStrippingTopology(SpoutType.KAFKA);
        }
        
        Config config = new Config();
        config.setDebug(false);

        Map<String, String> kafkaMetricsConfig = new HashMap<>();
        kafkaMetricsConfig.put(KafkaMetricsConsumer.KAFKA_BROKER_KEY, TextStrippingConstants.KAFKA_METRICS_BROKER);
        kafkaMetricsConfig.put(KafkaMetricsConsumer.KAFKA_TOPIC_KEY, TextStrippingConstants.KAFKA_METRICS_TOPIC);
        config.registerMetricsConsumer(KafkaMetricsConsumer.class, kafkaMetricsConfig, TextStrippingConstants.METRICS_CONSUMER_PARALLEL);
        
        StormTopology stormTopology = textStrippingTopology.buildTopology();
        
        if (args != null && args.length > 1) 
        {
            config.setNumWorkers(Integer.parseInt(args[1]));
            config.setMaxTaskParallelism(Integer.parseInt(args[2]));
            /*
            config.put(Config.NIMBUS_THRIFT_PORT, Integer.parseInt(args[6]));
            config.put(Config.STORM_ZOOKEEPER_PORT, Integer.parseInt(args[4]));
            config.put(Config.NIMBUS_HOST, args[5]);
            config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(args[3].split(";")));
            */
            
            if(args.length >= 4)
            {
                config.put(Config.TOPOLOGY_WORKER_CHILDOPTS, args[3]);
            }
            
            StormSubmitter.submitTopology(args[0], config, stormTopology);
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
