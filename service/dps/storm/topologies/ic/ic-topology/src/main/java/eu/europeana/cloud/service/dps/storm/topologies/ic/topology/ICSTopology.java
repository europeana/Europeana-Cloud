package eu.europeana.cloud.service.dps.storm.topologies.ic.topology;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import eu.europeana.cloud.service.dps.storm.*;
import eu.europeana.cloud.service.dps.storm.io.ReadFileBolt;
import eu.europeana.cloud.service.dps.storm.io.WriteRecordBolt;
import eu.europeana.cloud.service.dps.storm.topologies.ic.topology.bolt.IcBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.*;

import java.util.Arrays;

//import eu.europeana.cloud.service.dps.storm.ProgressBolt;

/**
 * This is the XSLT transformation topology for Apache Storm. The topology reads
 * from the cloud, download an XSLT sheet from a remote server, apply it to each
 * record read and save it back to the cloud.
 * <p/>
 * The topology takes some parameters. When deployed in distributed mode:
 * <p/>
 * args[0] is the name of the topology;
 * args[1] is the IP of the Storm Nimbus machine;
 * <p/>
 * args[2] is the IP of the zookeeper machine for storm
 * args[3] is the IP of the zookeeper machine for the dps
 * <p/>
 * args[4] is the Kafka topic where the xslt topology is listening from
 * <p/>
 * args[5] is the address of the MCS service
 * args[6] is the MCS username;
 * args[7] is the MCS password;
 *
 * @author Franco Maria Nardini (francomaria.nardini@isti.cnr.it)
 */
public class ICSTopology {

    private final static int WORKER_COUNT = 8;
    private final static int THRIFT_PORT = 6627;

    public static final Logger LOGGER = LoggerFactory.getLogger(ICSTopology.class);

    private final BrokerHosts brokerHosts;

    public ICSTopology(String kafkaZkAddress) {
        brokerHosts = new ZkHosts(kafkaZkAddress);
    }

    public StormTopology buildTopology(String dpsZkAddress, String xsltTopic,
                                       String ecloudMcsAddress, String username, String password) {

        ReadFileBolt retrieveFileBolt = new ReadFileBolt(ecloudMcsAddress, username, password);
        WriteRecordBolt writeRecordBolt = new WriteRecordBolt(ecloudMcsAddress, username, password);
        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, xsltTopic, "", "storm");
        kafkaConfig.forceFromStart = true;
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        TopologyBuilder builder = new TopologyBuilder();
        KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);
        builder.setSpout("kafkaReader", kafkaSpout, ICConstants.KAFKA_SPOUT_PARALLEL)
                .setNumTasks(ICConstants.NUMBER_OF_TASKS);

        builder.setBolt("parseKafkaInput", new ParseTaskBolt(),
                ICConstants.PARSE_TASKS_BOLT_PARALLEL).setNumTasks(ICConstants.NUMBER_OF_TASKS)
                .shuffleGrouping("kafkaReader");

        builder.setBolt("retrieveFileBolt", retrieveFileBolt, ICConstants.RETRIEVE_FILE_BOLT_PARALLEL)
                .setNumTasks(ICConstants.NUMBER_OF_TASKS).shuffleGrouping("parseKafkaInput");

        builder.setBolt("imageConversionBolt", new IcBolt(),
                ICConstants.XSLT_BOLT_PARALLEL).setNumTasks(ICConstants.NUMBER_OF_TASKS)
                .shuffleGrouping("retrieveFileBolt");

        builder.setBolt("writeRecordBolt", writeRecordBolt, ICConstants.WRITE_BOLT_PARALLEL)
                .setNumTasks(ICConstants.NUMBER_OF_TASKS)
                .shuffleGrouping("imageConversionBolt");

        builder.setBolt("endBolt", new EndBolt(), ICConstants.END_BOLT_PARALLEL).shuffleGrouping("writeRecordBolt");

        builder.setBolt("notificationBolt", new NotificationBolt(ICConstants.CASSANDRA_HOSTS,
                        ICConstants.CASSANDRA_PORT, ICConstants.CASSANDRA_KEYSPACE_NAME,
                        ICConstants.CASSANDRA_USERNAME, ICConstants.CASSANDRA_PASSWORD, true),
                ICConstants.NOTIFICATION_BOLT_PARALLEL)
                .fieldsGrouping("parseKafkaInput", AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping("retrieveFileBolt", AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping("imageConversionBolt", AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping("writeRecordBolt", AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping("endBolt", AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName));

        return builder.createTopology();
    }

    public static void main(String[] args) throws Exception {

        Config config = new Config();
        config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 2000);

        if (args != null && args.length == 5) {

            String topologyName = args[0];
            String nimbusHost = "localhost";
            String dpsZookeeper = args[1];
            String kafkaTopic = topologyName;
            String ecloudMcsAddress = args[2];
            String username = args[3];
            String password = args[4];

            ICSTopology kafkaSpoutTestTopology = new ICSTopology(dpsZookeeper);

            StormTopology stormTopology = kafkaSpoutTestTopology.buildTopology(dpsZookeeper,
                    kafkaTopic, ecloudMcsAddress, username, password);

            config.setNumWorkers(WORKER_COUNT);
            config.setMaxTaskParallelism(ICConstants.MAX_TASK_PARALLELISM);
            config.put(Config.NIMBUS_THRIFT_PORT, THRIFT_PORT);
            config.put(dpsZookeeper, ICConstants.INPUT_ZOOKEEPER_PORT);
            config.put(Config.NIMBUS_HOST, nimbusHost);
            config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(dpsZookeeper));
            System.out.println("before");
            StormSubmitter.submitTopology(topologyName, config, stormTopology);
            System.out.println("after");
        }
    }
}