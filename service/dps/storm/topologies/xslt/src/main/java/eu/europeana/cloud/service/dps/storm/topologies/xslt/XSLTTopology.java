package eu.europeana.cloud.service.dps.storm.topologies.xslt;

import eu.europeana.cloud.service.dps.InputDataType;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.ParseTaskBolt;
import eu.europeana.cloud.service.dps.storm.io.*;
import eu.europeana.cloud.service.dps.storm.topologies.properties.PropertyFileLoader;
import eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys;
import eu.europeana.cloud.service.dps.storm.utils.TopologyHelper;
import eu.europeana.cloud.service.dps.storm.xslt.XsltBolt;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


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
    private final String DATASET_STREAM = InputDataType.DATASET_URLS.name();
    private final String FILE_STREAM = InputDataType.FILE_URLS.name();


    public XSLTTopology(String defaultPropertyFile, String providedPropertyFile) {
        topologyProperties = new Properties();
        PropertyFileLoader.loadPropertyFile(defaultPropertyFile, providedPropertyFile, topologyProperties);
        brokerHosts = new ZkHosts(topologyProperties.getProperty(TopologyPropertyKeys.INPUT_ZOOKEEPER_ADDRESS));
    }

    public StormTopology buildTopology(String xsltTopic, String ecloudMcsAddress) {


        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, xsltTopic, "", "storm");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaConfig.ignoreZkOffsets = true;
        kafkaConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);
        TopologyBuilder builder = new TopologyBuilder();

        Map<String, String> routingRules = new HashMap<>();
        routingRules.put(DATASET_STREAM, DATASET_STREAM);
        routingRules.put(FILE_STREAM, FILE_STREAM);

        ReadFileBolt retrieveFileBolt = new ReadFileBolt(ecloudMcsAddress);
        WriteRecordBolt writeRecordBolt = new WriteRecordBolt(ecloudMcsAddress);
        RevisionWriterBolt revisionWriterBolt = new RevisionWriterBolt(ecloudMcsAddress);
        // TOPOLOGY STRUCTURE!
        builder.setSpout(TopologyHelper.SPOUT, kafkaSpout,
                ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.KAFKA_SPOUT_PARALLEL))))
                .setNumTasks(
                        ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.KAFKA_SPOUT_NUMBER_OF_TASKS))));

        builder.setBolt(TopologyHelper.PARSE_TASK_BOLT, new ParseTaskBolt(routingRules),
                ((int) Integer
                        .parseInt(topologyProperties.getProperty(TopologyPropertyKeys.PARSE_TASKS_BOLT_PARALLEL))))
                .setNumTasks(
                        ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.PARSE_TASKS_BOLT_NUMBER_OF_TASKS))))
                .shuffleGrouping(TopologyHelper.SPOUT);


        builder.setBolt(TopologyHelper.READ_DATASETS_BOLT, new ReadDatasetsBolt(),
                ((int) Integer
                        .parseInt(topologyProperties.getProperty(TopologyPropertyKeys.READ_DATASETS_BOLT_PARALLEL))))
                .setNumTasks(
                        ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.READ_DATASETS_BOLT_NUMBER_OF_TASKS))))
                .shuffleGrouping(TopologyHelper.PARSE_TASK_BOLT, DATASET_STREAM);

        builder.setBolt(TopologyHelper.READ_DATASET_BOLT, new ReadDatasetBolt(ecloudMcsAddress),
                ((int) Integer
                        .parseInt(topologyProperties.getProperty(TopologyPropertyKeys.READ_DATASET_BOLT_PARALLEL))))
                .setNumTasks(
                        ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.READ_DATASET_BOLT_NUMBER_OF_TASKS))))
                .shuffleGrouping(TopologyHelper.READ_DATASETS_BOLT);


        builder.setBolt(TopologyHelper.READ_REPRESENTATION_BOLT, new ReadRepresentationBolt(ecloudMcsAddress),
                ((int) Integer
                        .parseInt(topologyProperties.getProperty(TopologyPropertyKeys.READ_REPRESENTATION_BOLT_PARALLEL))))
                .setNumTasks(
                        ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.READ_REPRESENTATION_BOLT_NUMBER_OF_TASKS))))
                .shuffleGrouping(TopologyHelper.READ_DATASET_BOLT);


        builder.setBolt(TopologyHelper.RETRIEVE_FILE_BOLT, retrieveFileBolt,
                ((int) Integer
                        .parseInt(topologyProperties.getProperty(TopologyPropertyKeys.RETRIEVE_FILE_BOLT_PARALLEL))))
                .setNumTasks(
                        ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.RETRIEVE_FILE_BOLT_NUMBER_OF_TASKS))))
                .shuffleGrouping(TopologyHelper.PARSE_TASK_BOLT, FILE_STREAM).shuffleGrouping(TopologyHelper.READ_REPRESENTATION_BOLT);


        builder.setBolt(TopologyHelper.XSLT_BOLT, new XsltBolt(),
                ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.XSLT_BOLT_PARALLEL))))
                .setNumTasks(
                        ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.XSLT_BOLT_NUMBER_OF_TASKS))))
                .shuffleGrouping(TopologyHelper.RETRIEVE_FILE_BOLT);

        builder.setBolt(TopologyHelper.WRITE_RECORD_BOLT, writeRecordBolt,
                ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.WRITE_BOLT_PARALLEL))))
                .setNumTasks(
                        ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.WRITE_BOLT_NUMBER_OF_TASKS))))
                .shuffleGrouping(TopologyHelper.XSLT_BOLT);


        builder.setBolt(TopologyHelper.REVISION_WRITER_BOLT, revisionWriterBolt,
                ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.REVISION_WRITER_BOLT_PARALLEL))))
                .setNumTasks(
                        ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.Revision_WRITER_BOLT_NUMBER_OF_TASKS))))
                .shuffleGrouping(TopologyHelper.WRITE_RECORD_BOLT);

        AddResultToDataSetBolt addResultToDataSetBolt = new AddResultToDataSetBolt(ecloudMcsAddress);
        builder.setBolt(TopologyHelper.WRITE_TO_DATA_SET_BOLT, addResultToDataSetBolt,
                ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.ADD_TO_DATASET_BOLT_PARALLEL))))
                .setNumTasks(
                        ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.ADD_TO_DATASET_BOLT_NUMBER_OF_TASKS))))
                .shuffleGrouping(TopologyHelper.REVISION_WRITER_BOLT);


        builder.setBolt(TopologyHelper.NOTIFICATION_BOLT, new NotificationBolt(topologyProperties.getProperty(TopologyPropertyKeys.CASSANDRA_HOSTS),
                        Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.CASSANDRA_PORT)),
                        topologyProperties.getProperty(TopologyPropertyKeys.CASSANDRA_KEYSPACE_NAME),
                        topologyProperties.getProperty(TopologyPropertyKeys.CASSANDRA_USERNAME),
                        topologyProperties.getProperty(TopologyPropertyKeys.CASSANDRA_PASSWORD)),
                Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.NOTIFICATION_BOLT_PARALLEL)))
                .setNumTasks(
                        ((int) Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.NOTIFICATION_BOLT_NUMBER_OF_TASKS))))
                .fieldsGrouping(TopologyHelper.PARSE_TASK_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(TopologyHelper.RETRIEVE_FILE_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(TopologyHelper.READ_DATASETS_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(TopologyHelper.READ_DATASET_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(TopologyHelper.READ_REPRESENTATION_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(TopologyHelper.XSLT_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(TopologyHelper.WRITE_RECORD_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(TopologyHelper.REVISION_WRITER_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(TopologyHelper.WRITE_TO_DATA_SET_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
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

            XSLTTopology XsltTopology = new XSLTTopology(TOPOLOGY_PROPERTIES_FILE, providedPropertyFile);
            String topologyName = topologyProperties.getProperty(TopologyPropertyKeys.TOPOLOGY_NAME);

            // assuming kafka topic == topology name
            String kafkaTopic = topologyName;

            String ecloudMcsAddress = topologyProperties.getProperty(TopologyPropertyKeys.MCS_URL);

            StormTopology stormTopology = XsltTopology.buildTopology(
                    kafkaTopic,
                    ecloudMcsAddress);

            config.setNumWorkers(Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.WORKER_COUNT)));
            config.setMaxTaskParallelism(
                    Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.MAX_TASK_PARALLELISM)));
            config.put(Config.NIMBUS_THRIFT_PORT,
                    Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.THRIFT_PORT)));
            config.put(topologyProperties.getProperty(TopologyPropertyKeys.INPUT_ZOOKEEPER_ADDRESS),
                    topologyProperties.getProperty(TopologyPropertyKeys.INPUT_ZOOKEEPER_PORT));
            config.put(Config.NIMBUS_SEEDS, Arrays.asList(new String[]{topologyProperties.getProperty(TopologyPropertyKeys.NIMBUS_SEEDS)}));
            config.put(Config.STORM_ZOOKEEPER_SERVERS,
                    Arrays.asList(topologyProperties.getProperty(TopologyPropertyKeys.STORM_ZOOKEEPER_ADDRESS)));
            StormSubmitter.submitTopology(topologyName, config, stormTopology);
        }
    }
}