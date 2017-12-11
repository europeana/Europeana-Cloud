package eu.europeana.cloud.dps.topologies.media;

import java.util.Properties;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.topologies.properties.PropertyFileLoader;
import eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys;
import eu.europeana.cloud.service.dps.storm.utils.TopologyHelper;

public class MediaTopology {
	
	public static void main(String[] args)
			throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		Config conf = new Config();
		conf.setDebug(true);
		conf.setNumWorkers(2);
		
		Properties topologyProperties = new Properties();
		PropertyFileLoader.loadPropertyFile("media-topology-config.properties", "", topologyProperties);
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new NumberSpout(), 2);
		builder.setBolt("consumer", new NumberConsumer(), 3).shuffleGrouping("spout");
		
		builder.setBolt(TopologyHelper.NOTIFICATION_BOLT,
				new NotificationBolt(topologyProperties.getProperty(TopologyPropertyKeys.CASSANDRA_HOSTS),
						Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.CASSANDRA_PORT)),
						topologyProperties.getProperty(TopologyPropertyKeys.CASSANDRA_KEYSPACE_NAME),
						topologyProperties.getProperty(TopologyPropertyKeys.CASSANDRA_USERNAME),
						topologyProperties.getProperty(TopologyPropertyKeys.CASSANDRA_PASSWORD)),
				Integer.parseInt(topologyProperties.getProperty(TopologyPropertyKeys.NOTIFICATION_BOLT_PARALLEL)))
				.setNumTasks((Integer.parseInt(
						topologyProperties.getProperty(TopologyPropertyKeys.NOTIFICATION_BOLT_NUMBER_OF_TASKS))))
				.fieldsGrouping("consumer", AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
						new Fields(NotificationTuple.taskIdFieldName));
		
		if (args.length > 0) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("mediaTopology", conf, builder.createTopology());
			Utils.sleep(60000);
			cluster.killTopology("mediaTopology");
			cluster.shutdown();
		} else {
			StormSubmitter.submitTopology("mms-experiment", conf, builder.createTopology());
		}
		
	}
}
