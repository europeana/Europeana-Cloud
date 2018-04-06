package eu.europeana.cloud.service.dps.storm.topologies.oaipmh;

import eu.europeana.cloud.service.dps.InputDataType;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.ParseTaskBolt;
import eu.europeana.cloud.service.dps.storm.io.AddResultToDataSetBolt;
import eu.europeana.cloud.service.dps.storm.io.HarvestingWriteRecordBolt;
import eu.europeana.cloud.service.dps.storm.io.RevisionWriterBolt;
import eu.europeana.cloud.service.dps.storm.io.WriteRecordBolt;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.CustomKafkaSpout;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.IdentifiersHarvestingBolt;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.RecordHarvestingBolt;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.TaskSplittingBolt;
import eu.europeana.cloud.service.dps.storm.topologies.properties.PropertyFileLoader;
import eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys;
import eu.europeana.cloud.service.dps.storm.utils.TopologyHelper;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import com.google.common.base.Throwables;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.*;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.*;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;

/**
 *
 */
public class OAIPHMHarvestingTopology {
    private static Properties topologyProperties;
    private final BrokerHosts brokerHosts;
    private static final String TOPOLOGY_PROPERTIES_FILE = "oai-topology-config.properties";
    private final String REPOSITORY_STREAM = InputDataType.REPOSITORY_URLS.name();
    private static final Logger LOGGER = LoggerFactory.getLogger(OAIPHMHarvestingTopology.class);

    public OAIPHMHarvestingTopology(String defaultPropertyFile, String providedPropertyFile) {
        topologyProperties = new Properties();
        PropertyFileLoader.loadPropertyFile(defaultPropertyFile, providedPropertyFile, topologyProperties);
        brokerHosts = new ZkHosts(topologyProperties.getProperty(INPUT_ZOOKEEPER_ADDRESS));
    }

    public final StormTopology buildTopology(String oaiTopic, String ecloudMcsAddress, String uisAddress) {
        Map<String, String> routingRules = new HashMap<>();
        routingRules.put(REPOSITORY_STREAM, REPOSITORY_STREAM);


        WriteRecordBolt writeRecordBolt = new HarvestingWriteRecordBolt(ecloudMcsAddress, uisAddress);
        RevisionWriterBolt revisionWriterBolt = new RevisionWriterBolt(ecloudMcsAddress);
        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, oaiTopic, "", "storm");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaConfig.ignoreZkOffsets = true;
        kafkaConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        TopologyBuilder builder = new TopologyBuilder();
        CustomKafkaSpout kafkaSpout = new CustomKafkaSpout(kafkaConfig, topologyProperties.getProperty(TopologyPropertyKeys.CASSANDRA_HOSTS),
                Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.CASSANDRA_PORT)),
                topologyProperties.getProperty(TopologyPropertyKeys.CASSANDRA_KEYSPACE_NAME),
                topologyProperties.getProperty(TopologyPropertyKeys.CASSANDRA_USERNAME),
                topologyProperties.getProperty(TopologyPropertyKeys.CASSANDRA_PASSWORD));


        builder.setSpout(SPOUT, kafkaSpout, (getAnInt(KAFKA_SPOUT_PARALLEL)))
                .setNumTasks((getAnInt(KAFKA_SPOUT_NUMBER_OF_TASKS)));

        builder.setBolt(PARSE_TASK_BOLT, new ParseTaskBolt(routingRules),
                getAnInt(PARSE_TASKS_BOLT_PARALLEL))
                .setNumTasks(getAnInt(PARSE_TASKS_BOLT_NUMBER_OF_TASKS))
                .shuffleGrouping(SPOUT);

        builder.setBolt(TASK_SPLITTING_BOLT, new TaskSplittingBolt(parseLong(topologyProperties.getProperty(INTERVAL))),
                (getAnInt(TASK_SPLITTING_BOLT_PARALLEL)))
                .setNumTasks((getAnInt(TASK_SPLITTING_BOLT_BOLT_NUMBER_OF_TASKS)))
                .shuffleGrouping(PARSE_TASK_BOLT, REPOSITORY_STREAM);


        builder.setBolt(IDENTIFIERS_HARVESTING_BOLT, new IdentifiersHarvestingBolt(),
                (getAnInt(IDENTIFIERS_HARVESTING_BOLT_PARALLEL)))
                .setNumTasks((getAnInt(IDENTIFIERS_HARVESTING_BOLT_NUMBER_OF_TASKS)))
                .shuffleGrouping(TASK_SPLITTING_BOLT);


        builder.setBolt(RECORD_HARVESTING_BOLT, new RecordHarvestingBolt(),
                (getAnInt(RECORD_HARVESTING_BOLT_PARALLEL)))
                .setNumTasks((getAnInt(RECORD_HARVESTING_BOLT_NUMBER_OF_TASKS)))
                .shuffleGrouping(IDENTIFIERS_HARVESTING_BOLT);

        builder.setBolt(WRITE_RECORD_BOLT, writeRecordBolt,
                (getAnInt(WRITE_BOLT_PARALLEL)))
                .setNumTasks((getAnInt(WRITE_BOLT_NUMBER_OF_TASKS)))
                .shuffleGrouping(RECORD_HARVESTING_BOLT);

        builder.setBolt(REVISION_WRITER_BOLT, revisionWriterBolt,
                (getAnInt(REVISION_WRITER_BOLT_PARALLEL)))
                .setNumTasks((getAnInt(Revision_WRITER_BOLT_NUMBER_OF_TASKS)))
                .shuffleGrouping(WRITE_RECORD_BOLT);


        AddResultToDataSetBolt addResultToDataSetBolt = new AddResultToDataSetBolt(ecloudMcsAddress);
        builder.setBolt(WRITE_TO_DATA_SET_BOLT, addResultToDataSetBolt,
                (getAnInt(ADD_TO_DATASET_BOLT_PARALLEL)))
                .setNumTasks((getAnInt(ADD_TO_DATASET_BOLT_NUMBER_OF_TASKS)))
                .shuffleGrouping(REVISION_WRITER_BOLT);


        builder.setBolt(NOTIFICATION_BOLT, new NotificationBolt(topologyProperties.getProperty(CASSANDRA_HOSTS),
                        Integer.parseInt(topologyProperties.getProperty(CASSANDRA_PORT)),
                        topologyProperties.getProperty(CASSANDRA_KEYSPACE_NAME),
                        topologyProperties.getProperty(CASSANDRA_USERNAME),
                        topologyProperties.getProperty(CASSANDRA_PASSWORD)),
                Integer.parseInt(topologyProperties.getProperty(NOTIFICATION_BOLT_PARALLEL)))
                .setNumTasks(
                        ((int) Integer.parseInt(topologyProperties.getProperty(NOTIFICATION_BOLT_NUMBER_OF_TASKS))))
                .fieldsGrouping(PARSE_TASK_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(TASK_SPLITTING_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(IDENTIFIERS_HARVESTING_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(RECORD_HARVESTING_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(WRITE_RECORD_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(REVISION_WRITER_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(WRITE_TO_DATA_SET_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName));


        return builder.createTopology();
    }

    private static int getAnInt(String parseTasksBoltParallel) {
        return parseInt(topologyProperties.getProperty(parseTasksBoltParallel));
    }

    public static void main(String[] args) {
        try {

            if (args.length <= 1) {
                String providedPropertyFile = "";
                if (args.length == 1) {
                    providedPropertyFile = args[0];
                }
                OAIPHMHarvestingTopology oaiphmHarvestingTopology = new OAIPHMHarvestingTopology(TOPOLOGY_PROPERTIES_FILE, providedPropertyFile);
                String topologyName = topologyProperties.getProperty(TOPOLOGY_NAME);

                String kafkaTopic = topologyName;
                String ecloudMcsAddress = topologyProperties.getProperty(MCS_URL);
                String ecloudUisAddress = topologyProperties.getProperty(UIS_URL);
                StormTopology stormTopology = oaiphmHarvestingTopology.buildTopology(kafkaTopic, ecloudMcsAddress, ecloudUisAddress);
                StormSubmitter.submitTopology(topologyName, TopologyHelper.configureTopology(topologyProperties), stormTopology);
            }
        } catch (Exception e) {
            LOGGER.error(Throwables.getStackTraceAsString(e));
        }
    }
}
