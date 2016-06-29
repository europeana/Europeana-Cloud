
package eu.europeana.cloud.service.dps.xslt.kafka.topologies;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.metric.LoggingMetricsConsumer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import eu.europeana.cloud.service.dps.examples.StaticDpsTaskSpout;
import eu.europeana.cloud.service.dps.examples.util.DpsTaskUtil;
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
 * 
 *  Saves metrics by using {@LoggingMetricsConsumer}
 */
public class StaticXsltTopologyWithMetrics {
	
    private static String ecloudMcsAddress = "http://146.48.82.158:8080/ecloud-service-mcs-rest-0.3-SNAPSHOT";
	private static String username = "Cristiano";
	private static String password = "Ronaldo";
	
	private static String zkAddress = "ecloud.eanadev.org:2181";
    
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
		
		conf.registerMetricsConsumer(LoggingMetricsConsumer.class, 1);
		
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
