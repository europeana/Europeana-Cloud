package eu.europeana.cloud.service.dps.storm.topologies.xslt;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.io.*;
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
import eu.europeana.cloud.service.dps.storm.topologies.properties.PropertyFileLoader;
import eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys;
import eu.europeana.cloud.service.dps.storm.xslt.XsltBolt;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

/**
 * This is the XSLT transformation topology for Apache Storm. The topology reads
 * from the cloud, download an XSLT sheet from a remote server, apply it to each
 * record read and save it back to the cloud.
 *
 * @author Franco Maria Nardini (francomaria.nardini@isti.cnr.it)
 */
public class XSLTTopology {

    private static Properties topologyProperties;
    private final BrokerHosts brokerHosts;
    private final static String TOPOLOGY_PROPERTIES_FILE = "xslt-topology-config.properties";
    public static final Logger LOGGER = LoggerFactory.getLogger(XSLTTopology.class);
    private final String datasetStream = "DATASET_STREAM";
    private final String fileStream = "FILE_STREAM";

    public XSLTTopology(String defaultPropertyFile, String providedPropertyFile) {
        topologyProperties = new Properties();
        PropertyFileLoader.loadPropertyFile(defaultPropertyFile, providedPropertyFile, topologyProperties);
        brokerHosts = new ZkHosts(topologyProperties.getProperty(TopologyPropertyKeys.INPUT_ZOOKEEPER_ADDRESS));
    }

    public StormTopology buildTopology(String dpsZkAddress, String xsltTopic, String ecloudMcsAddress, String username,
                                       String password) {

        Map<String, String> routingRules = new HashMap<>();
        routingRules.put(PluginParameterKeys.FILE_URLS, datasetStream);
        routingRules.put(PluginParameterKeys.DATASET_URLS, fileStream);

        ReadFileBolt retrieveFileBolt = new ReadFileBolt(ecloudMcsAddress);
        WriteRecordBolt writeRecordBolt = new WriteRecordBolt(ecloudMcsAddress);
        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, xsltTopic, "", "storm");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaConfig.forceFromStart = true;
        kafkaConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        TopologyBuilder builder = new TopologyBuilder();
        KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);

        // TOPOLOGY STRUCTURE!
        builder.setSpout("kafkaReader", kafkaSpout,
                ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.KAFKA_SPOUT_PARALLEL))))
                .setNumTasks(
                        ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.NUMBER_OF_TASKS))));

        builder.setBolt("parseKafkaInput", new ParseTaskBolt(routingRules, null),
                ((int) Integer
                        .parseInt(topologyProperties.getProperty(TopologyPropertyKeys.PARSE_TASKS_BOLT_PARALLEL))))
                .setNumTasks(
                        ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.NUMBER_OF_TASKS))))
                .shuffleGrouping("kafkaReader");


        builder.setBolt("RetrieveDatasetBolt", new ReadDatasetBolt(ecloudMcsAddress),
                ((int) Integer
                        .parseInt(topologyProperties.getProperty(TopologyPropertyKeys.RETRIEVE_FILE_BOLT_PARALLEL))))
                .setNumTasks(
                        ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.NUMBER_OF_TASKS))))
                .shuffleGrouping("parseKafkaInput", datasetStream);

        builder.setBolt("retrieveFileBolt", retrieveFileBolt,
                ((int) Integer
                        .parseInt(topologyProperties.getProperty(TopologyPropertyKeys.RETRIEVE_FILE_BOLT_PARALLEL))))
                .setNumTasks(
                        ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.NUMBER_OF_TASKS))))
                .shuffleGrouping("parseKafkaInput", fileStream);

        builder.setBolt("xsltTransformationBolt", new XsltBolt(),
                ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.XSLT_BOLT_PARALLEL))))
                .setNumTasks(
                        ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.NUMBER_OF_TASKS))))
                .shuffleGrouping("RetrieveDatasetBolt")
                .shuffleGrouping("retrieveFileBolt");

        builder.setBolt("writeRecordBolt", writeRecordBolt,
                ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.WRITE_BOLT_PARALLEL))))
                .setNumTasks(
                        ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.NUMBER_OF_TASKS))))
                .shuffleGrouping("xsltTransformationBolt");

        builder.setBolt("notificationBolt",
                new NotificationBolt(topologyProperties.getProperty(TopologyPropertyKeys.CASSANDRA_HOSTS),
                        Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.CASSANDRA_PORT)),
                        topologyProperties.getProperty(TopologyPropertyKeys.CASSANDRA_KEYSPACE_NAME),
                        topologyProperties.getProperty(TopologyPropertyKeys.CASSANDRA_USERNAME),
                        topologyProperties.getProperty(TopologyPropertyKeys.CASSANDRA_PASSWORD), true),
                Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.NOTIFICATION_BOLT_PARALLEL)))
                .fieldsGrouping("parseKafkaInput", AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping("retrieveFileBolt", AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping("RetrieveDatasetBolt", AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping("xsltTransformationBolt", AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping("writeRecordBolt", AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName));


        // builder.setBolt("progressBolt", progressBolt,
        // 1).setNumTasks(1).shuffleGrouping("writeRecordBolt");
        // END OF TOPOLOGY STRUCTURE

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

            XSLTTopology XsltTopology = new XSLTTopology(TOPOLOGY_PROPERTIES_FILE, providedPropertyFile);
            String topologyName = topologyProperties.getProperty(TopologyPropertyKeys.TOPOLOGY_NAME);

            // assuming kafka topic == topology name
            String kafkaTopic = topologyName;

            String ecloudMcsAddress = topologyProperties.getProperty(TopologyPropertyKeys.MCS_URL);
            String username = topologyProperties.getProperty(TopologyPropertyKeys.MCS_USER_NAME);
            String password = topologyProperties.getProperty(TopologyPropertyKeys.MCS_USER_PASS);

            StormTopology stormTopology = XsltTopology.buildTopology(
                    topologyProperties.getProperty(TopologyPropertyKeys.INPUT_ZOOKEEPER_ADDRESS), kafkaTopic,
                    ecloudMcsAddress, username, password);

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