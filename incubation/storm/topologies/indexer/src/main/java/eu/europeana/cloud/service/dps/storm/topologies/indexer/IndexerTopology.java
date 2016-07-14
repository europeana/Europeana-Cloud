package eu.europeana.cloud.service.dps.storm.topologies.indexer;

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
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.index.SupportedIndexers;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.ParseTaskBolt;
import eu.europeana.cloud.service.dps.storm.io.ReadFileBolt;
import eu.europeana.cloud.service.dps.storm.kafka.KafkaMetricsConsumer;

/**
 * Storm topology for index data by {@link Indexer}.
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class IndexerTopology 
{
    public enum SpoutType
    {
        KAFKA,
        FEEDER
    }
    
    private final SpoutType spoutType;
    
    private final String extractedDataStream = "ReadData";
    private final String annotationStream = "ReadAssociation";
    private final String indexStream = "ReadFile";

    private final String ecloudMcsAddress = IndexerConstants.MCS_URL;
    private final String username = IndexerConstants.USERNAME;
    private final String password = IndexerConstants.PASSWORD;
    
    /**
     * Constructor of index topology.
     * @param spoutType spout type
     */
    public IndexerTopology(SpoutType spoutType) 
    {
        this.spoutType = spoutType;
    }
    
    private StormTopology buildTopology()
    {
        Map<String, String> routingRules = new HashMap<>();
        routingRules.put(PluginParameterKeys.NEW_EXTRACTED_DATA_MESSAGE, extractedDataStream);
        routingRules.put(PluginParameterKeys.NEW_ANNOTATION_MESSAGE, annotationStream);
        routingRules.put(PluginParameterKeys.INDEX_FILE_MESSAGE, indexStream);
        
        Map<String, String> prerequisites = new HashMap<>();
        prerequisites.put(PluginParameterKeys.INDEX_DATA, "True");
        prerequisites.put(PluginParameterKeys.INDEXER, null);
        
        Map<SupportedIndexers, String> indexersAddresses = new HashMap<>();
        indexersAddresses.put(SupportedIndexers.ELASTICSEARCH_INDEXER, IndexerConstants.ELASTICSEARCH_ADDRESSES);
        indexersAddresses.put(SupportedIndexers.SOLR_INDEXER, IndexerConstants.SOLR_ADDRESSES);
                
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("KafkaSpout", getSpout(), IndexerConstants.KAFKA_SPOUT_PARALLEL);
        
        builder.setBolt("ParseDpsTask", new ParseTaskBolt(routingRules, prerequisites), IndexerConstants.PARSE_TASKS_BOLT_PARALLEL)
                .shuffleGrouping("KafkaSpout");
        
        builder.setBolt("MergeDocuments", new MergeIndexedDocumentsBolt(indexersAddresses, IndexerConstants.CACHE_SIZE), IndexerConstants.PARSE_TASKS_BOLT_PARALLEL)
                .shuffleGrouping("ParseDpsTask", annotationStream);
        
        builder.setBolt("RetrieveFile", new ReadFileBolt(ecloudMcsAddress, username, password), 
                            IndexerConstants.FILE_BOLT_PARALLEL)
                .shuffleGrouping("ParseDpsTask", indexStream);
        
        builder.setBolt("IndexBolt", new IndexBolt(indexersAddresses, IndexerConstants.CACHE_SIZE), IndexerConstants.INDEX_BOLT_PARALLEL)             
                .shuffleGrouping("ParseDpsTask", extractedDataStream)
                .shuffleGrouping("MergeDocuments")
                .shuffleGrouping("RetrieveFile");


        builder.setBolt("NotificationBolt", new NotificationBolt(IndexerConstants.CASSANDRA_HOSTS, 
                            IndexerConstants.CASSANDRA_PORT, IndexerConstants.CASSANDRA_KEYSPACE_NAME,
                            IndexerConstants.CASSANDRA_USERNAME, IndexerConstants.CASSANDRA_PASSWORD, true), 
                            IndexerConstants.NOTIFICATION_BOLT_PARALLEL)
                .fieldsGrouping("ParseDpsTask", AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping("MergeDocuments", AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping("RetrieveFile", AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping("IndexBolt", AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName));

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
                    new ZkHosts(IndexerConstants.INPUT_ZOOKEEPER), 
                    IndexerConstants.KAFKA_INPUT_TOPIC, 
                    IndexerConstants.ZOOKEEPER_ROOT, UUID.randomUUID().toString());
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
        IndexerTopology indexerTopology = new IndexerTopology(SpoutType.KAFKA);
        
        Config config = new Config();
        config.setDebug(false);
/*
        Map<String, String> kafkaMetricsConfig = new HashMap<>();
        kafkaMetricsConfig.put(KafkaMetricsConsumer.KAFKA_BROKER_KEY, IndexerConstants.KAFKA_METRICS_BROKER);
        kafkaMetricsConfig.put(KafkaMetricsConsumer.KAFKA_TOPIC_KEY, IndexerConstants.KAFKA_METRICS_TOPIC);
        config.registerMetricsConsumer(KafkaMetricsConsumer.class, kafkaMetricsConfig, IndexerConstants.METRICS_CONSUMER_PARALLEL);
*/       
        StormTopology stormTopology = indexerTopology.buildTopology();

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
            cluster.submitTopology("IndexerTopology", config, stormTopology);
            Utils.sleep(6000000);
            cluster.killTopology("IndexerTopology");
            cluster.shutdown();
        }
    }
}
