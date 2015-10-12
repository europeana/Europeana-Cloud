package eu.europeana.cloud.service.dps.storm.topologies.xslt;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
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
//import eu.europeana.cloud.service.dps.storm.ProgressBolt;
import eu.europeana.cloud.service.dps.storm.io.ReadFileBolt;
import eu.europeana.cloud.service.dps.storm.io.WriteRecordBolt;
import eu.europeana.cloud.service.dps.storm.kafka.KafkaParseTaskBolt;
import eu.europeana.cloud.service.dps.storm.topologies.xslt.XSLTConstants;
import eu.europeana.cloud.service.dps.storm.xslt.XsltBolt;

/**
 * This is the XSLT transformation topology for Apache Storm. The topology reads
 * from the cloud, download an XSLT sheet from a remote server, apply it to each
 * record read and save it back to the cloud.
 * 
 * The topology takes some parameters. When deployed in distributed mode:
 * 
 * args[0] is the name of the topology;
 * args[1] is the IP of the Storm Nimbus machine;
 * 
 * args[2] is the IP of the zookeeper machine for storm
 * args[3] is the IP of the zookeeper machine for the dps
 * 
 * args[4] is the Kafka topic where the xslt topology is listening from
 *
 * args[5] is the address of the MCS service
 * args[6] is the MCS username;
 * args[7] is the MCS password;
 * 
 * @author Franco Maria Nardini (francomaria.nardini@isti.cnr.it)
 *
 */
public class XSLTTopology {
	
	private final static int WORKER_COUNT = 8;
	private final static int THRIFT_PORT = 6627;
	
	public static final Logger LOGGER = LoggerFactory.getLogger(XSLTTopology.class);

	private final BrokerHosts brokerHosts;

	public XSLTTopology(String kafkaZkAddress) {
		brokerHosts = new ZkHosts(kafkaZkAddress);
	}

	public StormTopology buildTopology(String dpsZkAddress, String xsltTopic, 
			String ecloudMcsAddress, String username, String password) {

		ReadFileBolt retrieveFileBolt = new ReadFileBolt(ecloudMcsAddress, username, password);
		WriteRecordBolt writeRecordBolt = new WriteRecordBolt(ecloudMcsAddress, username, password);
		
		//ProgressBolt progressBolt = new ProgressBolt(dpsZkAddress);

		SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, xsltTopic, "", "storm");
		kafkaConfig.forceFromStart = true;
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		TopologyBuilder builder = new TopologyBuilder();
		
		KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);

		// TOPOLOGY STRUCTURE!
		// 1 executor, i.e., 1 thread.
		// 1 task per executor
		builder.setSpout("kafkaReader", kafkaSpout, XSLTConstants.KAFKA_SPOUT_PARALLEL)
				.setNumTasks(XSLTConstants.NUMBER_OF_TASKS);

		builder.setBolt("parseKafkaInput", new ParseTaskBolt(),
				XSLTConstants.PARSE_TASKS_BOLT_PARALLEL).setNumTasks(XSLTConstants.NUMBER_OF_TASKS)
				.shuffleGrouping("kafkaReader");

		builder.setBolt("retrieveFileBolt", retrieveFileBolt, XSLTConstants.RETRIEVE_FILE_BOLT_PARALLEL)
				.setNumTasks(XSLTConstants.NUMBER_OF_TASKS).shuffleGrouping("parseKafkaInput");

		builder.setBolt("xsltTransformationBolt", new XsltBolt(),
				XSLTConstants.XSLT_BOLT_PARALLEL).setNumTasks(XSLTConstants.NUMBER_OF_TASKS)
				.shuffleGrouping("retrieveFileBolt");

		builder.setBolt("writeRecordBolt", writeRecordBolt, XSLTConstants.WRITE_BOLT_PARALLEL)
				.setNumTasks(XSLTConstants.NUMBER_OF_TASKS)
				.shuffleGrouping("xsltTransformationBolt");
		
		builder.setBolt("endBolt", new EndBolt(), XSLTConstants.END_BOLT_PARALLEL).shuffleGrouping("writeRecordBolt");

        builder.setBolt("notificationBolt", new NotificationBolt(XSLTConstants.CASSANDRA_HOSTS, 
                XSLTConstants.CASSANDRA_PORT, XSLTConstants.CASSANDRA_KEYSPACE_NAME,
                XSLTConstants.CASSANDRA_USERNAME, XSLTConstants.CASSANDRA_PASSWORD, true), 
                XSLTConstants.NOTIFICATION_BOLT_PARALLEL)
    .fieldsGrouping("parseKafkaInput", AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
    .fieldsGrouping("retrieveFileBolt", AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
    .fieldsGrouping("xsltTransformationBolt", AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
    .fieldsGrouping("writeRecordBolt", AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName))
    .fieldsGrouping("endBolt", AbstractDpsBolt.NOTIFICATION_STREAM_NAME, new Fields(NotificationTuple.taskIdFieldName));
		
		//builder.setBolt("progressBolt", progressBolt, 1).setNumTasks(1).shuffleGrouping("writeRecordBolt");
		// END OF TOPOLOGY STRUCTURE

		return builder.createTopology();
	}

	public static void main(String[] args) throws Exception {

		Config config = new Config();
		config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 2000);

		if (args != null && args.length == 3) {

			String topologyName = args[0];
			String nimbusHost = "localhost";
			
			//String stormZookeeper = args[2];
			//String dpsZookeeper = args[3];
			
			// assuming kafka topic == topology name
			String kafkaTopic = topologyName;
			String ecloudMcsAddress = XSLTConstants.MCS_URL;
			
			// before refactoring args[6], args[7]
			String username = args[1];
			String password = args[2];

			XSLTTopology kafkaSpoutTestTopology = new XSLTTopology(XSLTConstants.INPUT_ZOOKEEPER_ADDRESS);
			
			StormTopology stormTopology = kafkaSpoutTestTopology.buildTopology(XSLTConstants.INPUT_ZOOKEEPER_ADDRESS,
					kafkaTopic, ecloudMcsAddress, username, password);

			config.setNumWorkers(WORKER_COUNT);
			config.setMaxTaskParallelism(XSLTConstants.MAX_TASK_PARALLELISM);
			config.put(Config.NIMBUS_THRIFT_PORT, THRIFT_PORT);
			config.put(XSLTConstants.INPUT_ZOOKEEPER_ADDRESS, XSLTConstants.INPUT_ZOOKEEPER_PORT);
			config.put(Config.NIMBUS_HOST, nimbusHost);
			config.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(XSLTConstants.INPUT_ZOOKEEPER_ADDRESS));
			StormSubmitter.submitTopology(topologyName, config, stormTopology);
		}
	}
}
