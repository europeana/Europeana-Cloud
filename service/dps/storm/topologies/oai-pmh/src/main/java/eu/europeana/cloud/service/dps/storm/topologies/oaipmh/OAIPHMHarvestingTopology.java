package eu.europeana.cloud.service.dps.storm.topologies.oaipmh;

import eu.europeana.cloud.service.dps.InputDataType;
import eu.europeana.cloud.service.dps.storm.ParseTaskBolt;
import eu.europeana.cloud.service.dps.storm.io.AddResultToDataSetBolt;
import eu.europeana.cloud.service.dps.storm.io.OAIWriteRecordBolt;
import eu.europeana.cloud.service.dps.storm.io.RevisionWriterBolt;
import eu.europeana.cloud.service.dps.storm.io.WriteRecordBolt;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.IdentifiersHarvestingBolt;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.RecordHarvestingBolt;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.TaskSplittingBolt;
import eu.europeana.cloud.service.dps.storm.topologies.properties.PropertyFileLoader;
import eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys;
import eu.europeana.cloud.service.dps.storm.utils.TopologyHelper;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 *
 */
public class OAIPHMHarvestingTopology {
    private static Properties topologyProperties;
    private final BrokerHosts brokerHosts;
    private static final String TOPOLOGY_PROPERTIES_FILE = "oai-topology-config.properties";
    private final String REPOSITORY_STREAM = InputDataType.REPOSITORY_URLS.name();

    public OAIPHMHarvestingTopology(String defaultPropertyFile, String providedPropertyFile) {
        topologyProperties = new Properties();
        PropertyFileLoader.loadPropertyFile(defaultPropertyFile, providedPropertyFile, topologyProperties);
        brokerHosts = new ZkHosts(topologyProperties.getProperty(TopologyPropertyKeys.INPUT_ZOOKEEPER_ADDRESS));
    }

    public final StormTopology buildTopology(String oaiTopic, String ecloudMcsAddress, String uisAddress) {
        Map<String, String> routingRules = new HashMap<>();
        routingRules.put(REPOSITORY_STREAM, REPOSITORY_STREAM);


        WriteRecordBolt writeRecordBolt = new OAIWriteRecordBolt(ecloudMcsAddress, uisAddress);
        RevisionWriterBolt revisionWriterBolt = new RevisionWriterBolt(ecloudMcsAddress);
        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, oaiTopic, "", "storm");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        // kafkaConfig.ignoreZkOffsets = true;
        kafkaConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        TopologyBuilder builder = new TopologyBuilder();
        KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);


        builder.setSpout(TopologyHelper.SPOUT, kafkaSpout,
                (Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.KAFKA_SPOUT_PARALLEL))))
                .setNumTasks(
                        (Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.KAFKA_SPOUT_NUMBER_OF_TASKS))));

        builder.setBolt(TopologyHelper.PARSE_TASK_BOLT, new ParseTaskBolt(routingRules),
                (Integer
                        .parseInt(topologyProperties.getProperty(TopologyPropertyKeys.PARSE_TASKS_BOLT_PARALLEL))))
                .setNumTasks(
                        (Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.PARSE_TASKS_BOLT_NUMBER_OF_TASKS))))
                .shuffleGrouping(TopologyHelper.SPOUT);


        builder.setBolt(TopologyHelper.TASK_SPLITTING_BOLT, new TaskSplittingBolt(Long.parseLong(topologyProperties.getProperty(TopologyPropertyKeys.INTERVAL))),
                (Integer
                        .parseInt(topologyProperties.getProperty(TopologyPropertyKeys.TASK_SPLITTING_BOLT_PARALLEL))))
                .setNumTasks(
                        (Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.TASK_SPLITTING_BOLT_BOLT_NUMBER_OF_TASKS))))
                .shuffleGrouping(TopologyHelper.PARSE_TASK_BOLT, REPOSITORY_STREAM);


        builder.setBolt(TopologyHelper.IDENTIFIERS_HARVESTING_BOLT, new IdentifiersHarvestingBolt(),
                (Integer
                        .parseInt(topologyProperties.getProperty(TopologyPropertyKeys.IDENTIFIERS_HARVESTING_BOLT_PARALLEL))))
                .setNumTasks(
                        (Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.IDENTIFIERS_HARVESTING_BOLT_NUMBER_OF_TASKS))))
                .shuffleGrouping(TopologyHelper.TASK_SPLITTING_BOLT);


        builder.setBolt(TopologyHelper.RECORD_HARVESTING_BOLT, new RecordHarvestingBolt(),
                (Integer
                        .parseInt(topologyProperties.getProperty(TopologyPropertyKeys.RECORD_HARVESTING_BOLT_PARALLEL))))
                .setNumTasks(
                        (Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.RECORD_HARVESTING_BOLT_NUMBER_OF_TASKS))))
                .shuffleGrouping(TopologyHelper.IDENTIFIERS_HARVESTING_BOLT);

        builder.setBolt(TopologyHelper.WRITE_RECORD_BOLT, writeRecordBolt,
                (Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.WRITE_BOLT_PARALLEL))))
                .setNumTasks(
                        (Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.WRITE_BOLT_NUMBER_OF_TASKS))))
                .shuffleGrouping(TopologyHelper.RECORD_HARVESTING_BOLT);

        builder.setBolt(TopologyHelper.REVISION_WRITER_BOLT, revisionWriterBolt,
                (Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.REVISION_WRITER_BOLT_PARALLEL))))
                .setNumTasks(
                        (Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.Revision_WRITER_BOLT_NUMBER_OF_TASKS))))
                .shuffleGrouping(TopologyHelper.WRITE_RECORD_BOLT);


        AddResultToDataSetBolt addResultToDataSetBolt = new AddResultToDataSetBolt(ecloudMcsAddress);
        builder.setBolt(TopologyHelper.WRITE_TO_DATA_SET_BOLT, addResultToDataSetBolt,
                (Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.ADD_TO_DATASET_BOLT_PARALLEL))))
                .setNumTasks(
                        (Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.ADD_TO_DATASET_BOLT_NUMBER_OF_TASKS))))
                .shuffleGrouping(TopologyHelper.REVISION_WRITER_BOLT);
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
            OAIPHMHarvestingTopology oaiphmHarvestingTopology = new OAIPHMHarvestingTopology(TOPOLOGY_PROPERTIES_FILE, providedPropertyFile);
            String topologyName = topologyProperties.getProperty(TopologyPropertyKeys.TOPOLOGY_NAME);

            String kafkaTopic = topologyName;
            String ecloudMcsAddress = topologyProperties.getProperty(TopologyPropertyKeys.MCS_URL);
            String ecloudUisAddress = topologyProperties.getProperty(TopologyPropertyKeys.UIS_URL);
            StormTopology stormTopology = oaiphmHarvestingTopology.buildTopology(kafkaTopic, ecloudMcsAddress, ecloudUisAddress);
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
