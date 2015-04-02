
package eu.europeana.cloud.service.dps.xslt.kafka.topologies;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import eu.europeana.cloud.service.dps.examples.StaticDpsTaskSpout;
import eu.europeana.cloud.service.dps.examples.util.DpsTaskUtil;
import eu.europeana.cloud.service.dps.storm.KafkaMetricsConsumer;
import eu.europeana.cloud.service.dps.storm.ProgressBolt;
import eu.europeana.cloud.service.dps.storm.io.ReadFileBolt;
import eu.europeana.cloud.service.dps.storm.io.WriteRecordBolt;
import eu.europeana.cloud.service.dps.storm.xslt.XsltBolt;

/**
 * Example ecloud topology:
 * 
 * - Creates a DpsTask using {@link StaticDpsTaskSpout}
 * 
 * - Reads a File from eCloud
 * 
 * - Writes a File to eCloud
 */
public class StaticXsltTopologyWithProgressReport {
	
    private static String ecloudMcsAddress = "http://146.48.82.158:8080/ecloud-service-mcs-rest-0.3-SNAPSHOT";
	private static String username = "Cristiano";
	private static String password = "Ronaldo";

	private static String kafkaTopic = "storm_metrics_topic";
	private static String kafkaBroker = "ecloud.eanadev.org:9093";
	
	private static String zkAddress = "ecloud.eanadev.org:2181";

	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();
		
		StaticDpsTaskSpout taskSpout = new StaticDpsTaskSpout(DpsTaskUtil.generateDpsTask());
		
		ReadFileBolt retrieveFileBolt = new ReadFileBolt(zkAddress, ecloudMcsAddress, username, password);
		WriteRecordBolt writeRecordBolt = new WriteRecordBolt(ecloudMcsAddress, username, password);
		ProgressBolt progressBolt = new ProgressBolt(zkAddress);

		builder.setSpout("taskSpout", taskSpout, 1);
		
		builder.setBolt("retrieveFileBolt", retrieveFileBolt, 2).shuffleGrouping(
				"taskSpout");
		
		builder.setBolt("xsltTransformationBolt", new XsltBolt(), 2).shuffleGrouping(
				"retrieveFileBolt");
		
		builder.setBolt("writeRecordBolt", writeRecordBolt, 2).shuffleGrouping(
				"xsltTransformationBolt");

		builder.setBolt("progressBolt", progressBolt, 1).shuffleGrouping(
				"writeRecordBolt");
 
		Config conf = new Config();
		conf.setDebug(true);
		conf.put(Config.TOPOLOGY_DEBUG, true);
		
	    Map<String, String> kafkaMetricsConfig = new HashMap<String, String>();
	    kafkaMetricsConfig.put(KafkaMetricsConsumer.KAFKA_BROKER_KEY, kafkaBroker);
	    kafkaMetricsConfig.put(KafkaMetricsConsumer.KAFKA_TOPIC_KEY, kafkaTopic);
	    conf.registerMetricsConsumer(KafkaMetricsConsumer.class, kafkaMetricsConfig, 60);
	    
		if (args != null && args.length > 0) {

			conf.setNumWorkers(3);

			StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
					builder.createTopology());
		} else {

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, builder.createTopology());
			Utils.sleep(60000);
			cluster.killTopology("test");
			cluster.shutdown();
		}
	}
}
