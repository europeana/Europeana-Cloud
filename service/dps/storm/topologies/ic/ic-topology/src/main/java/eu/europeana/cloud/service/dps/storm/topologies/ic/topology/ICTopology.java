package eu.europeana.cloud.service.dps.storm.topologies.ic.topology;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.io.*;
import eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.ParseTaskBolt;
import eu.europeana.cloud.service.dps.storm.topologies.ic.topology.bolt.IcBolt;
import eu.europeana.cloud.service.dps.storm.topologies.properties.PropertyFileLoader;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

/**
 * This is the Image conversion topology . The topology reads from the cloud,
 * apply Kakadu conversion to each record which was read and save it back to the
 * cloud.
 */

public class ICTopology {

    private static Properties topologyProperties;
    private final BrokerHosts brokerHosts;
    private final static String TOPOLOGY_PROPERTIES_FILE = "ic-topology-config.properties";
    public static final Logger LOGGER = LoggerFactory.getLogger(ICTopology.class);

    private final String kafkaReader = "kafkaReader";
    private final String parseKafkaInput = "parseKafkaInput";
    private final String readDatasetsBolt = "RetrieveDatasetBolt";
    private final String RetrieveFileBolt = "RetrieveFileBolt";
    private final String imageConversionBolt = "imageConversionBolt";
    private final String endBolt = "endBolt";
    private final String WriteRecordBolt = "WriteRecordBolt";
    private final String notificationBolt = "notificationBolt";
    private final String readDataSetBolt = "ReadDataSetBolt";
    private final String readRepresentationBolt = "ReadRepresentationBolt";

    private final String datasetStream = DpsTask.DATASET_URLS;
    private final String fileStream = DpsTask.FILE_URLS;

    public ICTopology(String defaultPropertyFile, String providedPropertyFile) {
        topologyProperties = new Properties();
        PropertyFileLoader.loadPropertyFile(defaultPropertyFile, providedPropertyFile, topologyProperties);
        brokerHosts = new ZkHosts(topologyProperties.getProperty(TopologyPropertyKeys.INPUT_ZOOKEEPER_ADDRESS));
    }

    public StormTopology buildTopology(String icTopic, String ecloudMcsAddress) {
        Map<String, String> routingRules = new HashMap<>();
        routingRules.put(PluginParameterKeys.FILE_URLS, datasetStream);
        routingRules.put(PluginParameterKeys.DATASET_URLS, fileStream);
        ReadFileBolt retrieveFileBolt = new ReadFileBolt(ecloudMcsAddress);
        WriteRecordBolt writeRecordBolt = new WriteRecordBolt(ecloudMcsAddress);
        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, icTopic, "", "storm");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaConfig.forceFromStart = true;
        kafkaConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        TopologyBuilder builder = new TopologyBuilder();
        KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);
        builder.setSpout(kafkaReader, kafkaSpout,
                ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.KAFKA_SPOUT_PARALLEL))))
                .setNumTasks(
                        ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.NUMBER_OF_TASKS))));
        builder.setBolt("parseKafkaInput", new ParseTaskBolt(routingRules),
                ((int) Integer
                        .parseInt(topologyProperties.getProperty(TopologyPropertyKeys.PARSE_TASKS_BOLT_PARALLEL))))
                .setNumTasks(
                        ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.NUMBER_OF_TASKS))))
                .shuffleGrouping(kafkaReader);


        builder.setBolt(readDatasetsBolt, new ReadDatasetsBolt(),
                ((int) Integer
                        .parseInt(topologyProperties.getProperty(TopologyPropertyKeys.RETRIEVE_FILE_BOLT_PARALLEL))))
                .setNumTasks(
                        ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.NUMBER_OF_TASKS))))
                .shuffleGrouping(parseKafkaInput, datasetStream);

        builder.setBolt(readDataSetBolt, new ReadDataSetBolt(ecloudMcsAddress),
                ((int) Integer
                        .parseInt(topologyProperties.getProperty(TopologyPropertyKeys.RETRIEVE_FILE_BOLT_PARALLEL))))
                .setNumTasks(
                        ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.NUMBER_OF_TASKS))))
                .shuffleGrouping(readDatasetsBolt);


        builder.setBolt(readRepresentationBolt, new ReadRepresentationBolt(ecloudMcsAddress),
                ((int) Integer
                        .parseInt(topologyProperties.getProperty(TopologyPropertyKeys.RETRIEVE_FILE_BOLT_PARALLEL))))
                .setNumTasks(
                        ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.NUMBER_OF_TASKS))))
                .shuffleGrouping(readDataSetBolt);

        builder.setBolt(RetrieveFileBolt, retrieveFileBolt,
                ((int) Integer
                        .parseInt(topologyProperties.getProperty(TopologyPropertyKeys.RETRIEVE_FILE_BOLT_PARALLEL))))
                .setNumTasks(
                        ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.NUMBER_OF_TASKS))))
                .shuffleGrouping(parseKafkaInput, fileStream).shuffleGrouping(readRepresentationBolt);

        builder.setBolt(imageConversionBolt, new IcBolt(),
                ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.IC_BOLT_PARALLEL))))
                .setNumTasks(((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.NUMBER_OF_TASKS))))
                .shuffleGrouping(RetrieveFileBolt);

        builder.setBolt(WriteRecordBolt, writeRecordBolt,
                ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.WRITE_BOLT_PARALLEL))))
                .setNumTasks(
                        ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.NUMBER_OF_TASKS))))
                .shuffleGrouping(imageConversionBolt);


        builder.setBolt(notificationBolt, new NotificationBolt(topologyProperties.getProperty(TopologyPropertyKeys.CASSANDRA_HOSTS),
                        Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.CASSANDRA_PORT)),
                        topologyProperties.getProperty(TopologyPropertyKeys.CASSANDRA_KEYSPACE_NAME),
                        topologyProperties.getProperty(TopologyPropertyKeys.CASSANDRA_USERNAME),
                        topologyProperties.getProperty(TopologyPropertyKeys.CASSANDRA_PASSWORD), true),
                Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.NOTIFICATION_BOLT_PARALLEL)))
                .fieldsGrouping(parseKafkaInput, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(RetrieveFileBolt, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(readDatasetsBolt, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(readDataSetBolt, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(readRepresentationBolt, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(imageConversionBolt, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping("writeRecordBolt", AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName));


        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {
        Config config = new Config();
        config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 2000);

        if (args.length <= 1) {

            String providedPropertyFile = "";
            if (args.length == 1) {
                providedPropertyFile = args[0];
            }
            ICTopology icTopology = new ICTopology(TOPOLOGY_PROPERTIES_FILE, providedPropertyFile);
            String topologyName = topologyProperties.getProperty(TopologyPropertyKeys.TOPOLOGY_NAME);
            // kafka topic == topology name
            String kafkaTopic = topologyName;
            String ecloudMcsAddress = topologyProperties.getProperty(TopologyPropertyKeys.MCS_URL);
            StormTopology stormTopology = icTopology.buildTopology(kafkaTopic, ecloudMcsAddress);
            config.setNumWorkers(Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.WORKER_COUNT)));
            config.setMaxTaskParallelism(
                    Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.MAX_TASK_PARALLELISM)));
            config.put(Config.NIMBUS_THRIFT_PORT,
                    Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.THRIFT_PORT)));
            config.put(topologyProperties.getProperty(TopologyPropertyKeys.INPUT_ZOOKEEPER_ADDRESS),
                    topologyProperties.getProperty(TopologyPropertyKeys.INPUT_ZOOKEEPER_PORT));
            config.put(Config.NIMBUS_HOST, "localhost");
            config.put(Config.STORM_ZOOKEEPER_SERVERS,
                    Arrays.asList(topologyProperties.getProperty(TopologyPropertyKeys.STORM_ZOOKEEPER_ADDRESS)));

            StormSubmitter.submitTopology(topologyName, config, stormTopology);
        }
    }
}
