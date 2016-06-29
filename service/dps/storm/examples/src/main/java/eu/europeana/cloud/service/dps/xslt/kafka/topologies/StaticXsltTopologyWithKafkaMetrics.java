
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
import eu.europeana.cloud.service.dps.storm.io.ReadFileBolt;
import eu.europeana.cloud.service.dps.storm.io.WriteRecordBolt;
import eu.europeana.cloud.service.dps.storm.kafka.KafkaMetricsConsumer;
import eu.europeana.cloud.service.dps.storm.xslt.XsltBolt;

/**
 * Example ecloud topology:
 * 
 * - Creates a DpsTask using {@link StaticDpsTaskSpout}
 * 
 * - Reads a File from eCloud
 * 
 * - Writes a File to eCloud
 * 
 * Tests storing of Metrics to Kafka
 */
public class StaticXsltTopologyWithKafkaMetrics {

    private static String ecloudMcsAddress_POLAND = "http://felicia.man.poznan.pl/mcs";
    
    private static String ecloudMcsAddress_ISTI = "http://146.48.82.158:8080/ecloud-service-mcs-rest-0.3-SNAPSHOT";
    
    private static String ecloudMcsAddress = ecloudMcsAddress_POLAND;

	private static String zkAddress = "ecloud.eanadev.org:2181";
    
	private static String username_ISTI = "Cristiano";
	private static String password_ISTI = "Ronaldo";
	
	private static String username_poland = "Emmanouil_Koufakis";
	private static String password_poland = "J9vdq9rpPy";
	
	private static String username = username_poland;
	private static String password = password_poland;

	private static String kafkaTopic = "storm_metrics_topic";
	private static String kafkaBroker = "ecloud.eanadev.org:9093";
	

	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();
		
		StaticDpsTaskSpout taskSpout = new StaticDpsTaskSpout(DpsTaskUtil.generateDpsTask());
		
		ReadFileBolt retrieveFileBolt = new ReadFileBolt(ecloudMcsAddress);
		WriteRecordBolt writeRecordBolt = new WriteRecordBolt(ecloudMcsAddress);

		builder.setSpout("taskSpout", taskSpout, 1);
		
		builder.setBolt("retrieveFileBolt", retrieveFileBolt, 1).shuffleGrouping(
				"taskSpout");
		
		builder.setBolt("xsltTransformationBolt", new XsltBolt(), 1).shuffleGrouping(
				"retrieveFileBolt");
		
		builder.setBolt("writeRecordBolt", writeRecordBolt, 1).shuffleGrouping(
				"xsltTransformationBolt");
 
		Config conf = new Config();
		conf.setDebug(true);
		conf.put(Config.TOPOLOGY_DEBUG, true);
		
	    Map<String, String> kafkaMetricsConfig = new HashMap<String, String>();
	    kafkaMetricsConfig.put(KafkaMetricsConsumer.KAFKA_BROKER_KEY, kafkaBroker);
	    kafkaMetricsConfig.put(KafkaMetricsConsumer.KAFKA_TOPIC_KEY, kafkaTopic);
	    conf.registerMetricsConsumer(KafkaMetricsConsumer.class, kafkaMetricsConfig, 1);
	    
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
