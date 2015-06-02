
package eu.europeana.cloud.service.dps.xslt.kafka.topologies;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
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
 * 
 * - Updates the progress of the submitted task
 */
public class StaticXsltTopologyWithProgressReport {
	
	/**
	 * Builds the topology
	 * @return Storm Xslt Topology
	 */
	private static StormTopology buildStaticXsltTopologyWithProgressReport(String[] args) {
		
		TopologyBuilder builder = new TopologyBuilder();
		
		StaticDpsTaskSpout taskSpout = new StaticDpsTaskSpout(DpsTaskUtil.generateDpsTask(args[5], args[6], Integer.parseInt(args[7])));
		
		ReadFileBolt retrieveFileBolt = new ReadFileBolt(args[1], args[2], args[3], args[4]);
		WriteRecordBolt writeRecordBolt = new WriteRecordBolt(args[2], args[3], args[4]);
		ProgressBolt progressBolt = new ProgressBolt(args[1]);

		builder.setSpout("taskSpout", taskSpout, 1);
		
		builder.setBolt("retrieveFileBolt", retrieveFileBolt, 2).shuffleGrouping(
				"taskSpout");
		
		builder.setBolt("xsltTransformationBolt", new XsltBolt(), 2).shuffleGrouping(
				"retrieveFileBolt");
		
		builder.setBolt("writeRecordBolt", writeRecordBolt, 2).shuffleGrouping(
				"xsltTransformationBolt");

		builder.setBolt("progressBolt", progressBolt, 1).shuffleGrouping(
				"writeRecordBolt");
 
	    return builder.createTopology();
	}

	public static void main(String[] args) throws Exception {

		Config conf = new Config();
		conf.setDebug(true);
		conf.put(Config.TOPOLOGY_DEBUG, true);
		
		if (args != null && args.length > 0) {

			conf.setNumWorkers(3);

			StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
					buildStaticXsltTopologyWithProgressReport(args));
		} 
	}
}
