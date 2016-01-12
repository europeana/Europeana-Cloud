package eu.europeana.cloud.service.dps.storm.topologies.ic.topology;

import java.util.Arrays;
import java.util.Properties;

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
import eu.europeana.cloud.service.dps.storm.EndBolt;
import eu.europeana.cloud.service.dps.storm.NotificationBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.ParseTaskBolt;
import eu.europeana.cloud.service.dps.storm.io.GrantPermissionsToFileBolt;
import eu.europeana.cloud.service.dps.storm.io.ReadFileBolt;
import eu.europeana.cloud.service.dps.storm.io.RemovePermissionsToFileBolt;
import eu.europeana.cloud.service.dps.storm.io.WriteRecordBolt;
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

public class ICSTopology {

    private static Properties topologyProperties;
    private final BrokerHosts brokerHosts;
    private final static String TOPOLOGY_PROPERTIES_FILE = "ic-topology-config.properties";
    public static final Logger LOGGER = LoggerFactory.getLogger(ICSTopology.class);

    public ICSTopology(String defaultPropertyFile, String providedPropertyFile) {
        topologyProperties = new Properties();
        PropertyFileLoader.loadPropertyFile(defaultPropertyFile, providedPropertyFile, topologyProperties);
        brokerHosts = new ZkHosts(topologyProperties.getProperty(TopologyPropertyKeys.INPUT_ZOOKEEPER_ADDRESS));
    }

    public StormTopology buildTopology(String icTopic, String ecloudMcsAddress, String username, String password) {

        ReadFileBolt retrieveFileBolt = new ReadFileBolt(ecloudMcsAddress, username, password);
        WriteRecordBolt writeRecordBolt = new WriteRecordBolt(ecloudMcsAddress, username, password);

        GrantPermissionsToFileBolt grantPermBolt = new GrantPermissionsToFileBolt(ecloudMcsAddress, username, password);
        RemovePermissionsToFileBolt removePermBolt = new RemovePermissionsToFileBolt(ecloudMcsAddress, username,
                password);
        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, icTopic, "", "storm");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaConfig.forceFromStart = true;
        kafkaConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        TopologyBuilder builder = new TopologyBuilder();
        KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);
        builder.setSpout("kafkaReader", kafkaSpout,
                ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.KAFKA_SPOUT_PARALLEL))))
                .setNumTasks(
                        ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.NUMBER_OF_TASKS))));

        builder.setBolt("parseKafkaInput", new ParseTaskBolt(),
                ((int) Integer
                        .parseInt(topologyProperties.getProperty(TopologyPropertyKeys.PARSE_TASKS_BOLT_PARALLEL))))
                .setNumTasks(
                        ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.NUMBER_OF_TASKS))))
                .shuffleGrouping("kafkaReader");

        builder.setBolt("retrieveFileBolt", retrieveFileBolt,
                ((int) Integer
                        .parseInt(topologyProperties.getProperty(TopologyPropertyKeys.RETRIEVE_FILE_BOLT_PARALLEL))))
                .setNumTasks(
                        ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.NUMBER_OF_TASKS))))
                .shuffleGrouping("parseKafkaInput");

        builder.setBolt("imageConversionBolt", new IcBolt(),
                ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.IC_BOLT_PARALLEL))))
                .setNumTasks(((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.NUMBER_OF_TASKS))))
                .shuffleGrouping("retrieveFileBolt");

        builder.setBolt("writeRecordBolt", writeRecordBolt,
                ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.WRITE_BOLT_PARALLEL))))
                .setNumTasks(
                        ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.NUMBER_OF_TASKS))))
                .shuffleGrouping("imageConversionBolt");

        builder.setBolt("grantPermBolt", grantPermBolt,
                ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.GRANT_BOLT_PARALLEL))))
                .setNumTasks(
                        ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.NUMBER_OF_TASKS))))
                .shuffleGrouping("writeRecordBolt");

        builder.setBolt("removePermBolt", removePermBolt,
                ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.REMOVE_BOLT_PARALLEL))))
                .setNumTasks(
                        ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.NUMBER_OF_TASKS))))
                .shuffleGrouping("grantPermBolt");

        builder.setBolt("endBolt", new EndBolt(),
                ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.END_BOLT_PARALLEL))))
                .shuffleGrouping("removePermBolt");

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
                .fieldsGrouping("imageConversionBolt", AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping("writeRecordBolt", AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping("grantPermBolt", AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping("removePermBolt", AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping("endBolt", AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
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
            ICSTopology icsTopology = new ICSTopology(TOPOLOGY_PROPERTIES_FILE, providedPropertyFile);
            String topologyName = topologyProperties.getProperty(TopologyPropertyKeys.TOPOLOGY_NAME);

            // kafka topic == topology name
            String kafkaTopic = topologyName;

            String ecloudMcsAddress = topologyProperties.getProperty(TopologyPropertyKeys.MCS_URL);
            String username = topologyProperties.getProperty(TopologyPropertyKeys.MCS_USER_NAME);
            String password = topologyProperties.getProperty(TopologyPropertyKeys.MCS_USER_PASS);
            StormTopology stormTopology = icsTopology.buildTopology(kafkaTopic, ecloudMcsAddress, username, password);
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
