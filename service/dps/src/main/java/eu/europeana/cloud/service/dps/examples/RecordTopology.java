
package eu.europeana.cloud.service.dps.examples;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

/**
 * Example ecloud topology:
 * 
 * - Reads a Record using {@link RandomRecordSpout}
 * and passes the Record as a byte array to the {@link CreateRecordBolt}
 * 
 * - Creates a new Record ({@link CreateRecordBolt})
 * and passes the URL of the newly created Record.
 */
public class RecordTopology {

	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("recordProviderSpout", new RandomRecordSpout(), 10);
		builder.setBolt("createRecordBolt", new CreateRecordBolt(), 3).shuffleGrouping(
				"recordProviderSpout");

		Config conf = new Config();
		conf.setDebug(true);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);

			StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
					builder.createTopology());
		} else {

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, builder.createTopology());
			Utils.sleep(10000);
			cluster.killTopology("test");
			cluster.shutdown();
		}
	}
}
