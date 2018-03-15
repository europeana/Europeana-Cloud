package eu.europeana.cloud.dps.topologies.media;

import java.util.Arrays;
import java.util.Collection;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.grouping.ShuffleGrouping;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import eu.europeana.cloud.dps.topologies.media.support.DummySpout;
import eu.europeana.cloud.dps.topologies.media.support.MediaTupleData.UrlType;
import eu.europeana.cloud.dps.topologies.media.support.StatsInitTupleData;
import eu.europeana.cloud.dps.topologies.media.support.StatsTupleData;
import eu.europeana.cloud.dps.topologies.media.support.Util;
import eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys;

public class MediaTopology {
	
	public static void main(String[] args)
			throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		
		Config conf = Util.loadConfig();
		
		final boolean isTest = args.length > 0;
		
		TopologyBuilder builder = new TopologyBuilder();
		String topologyName = (String) conf.computeIfAbsent(TopologyPropertyKeys.TOPOLOGY_NAME, k -> "media_topology");
		
		IRichSpout baseSpout = isTest ? new DummySpout() : new KafkaSpout(Util.getKafkaSpoutConfig(conf));
		Collection<UrlType> urlTypes = Arrays.asList(UrlType.OBJECT, UrlType.HAS_VIEW, UrlType.IS_SHOWN_BY);
		builder.setSpout("source", new DataSetReaderSpout(baseSpout, urlTypes), 1);
		
		builder.setBolt("downloadBolt", new DownloadBolt(), (Number) conf.get(Config.TOPOLOGY_WORKERS))
				.fieldsGrouping("source", new Fields(DataSetReaderSpout.SOURCE_FIELD));
		builder.setBolt("processingBolt", new ProcessingBolt(),
				(int) conf.get("MEDIATOPOLOGY_PARALLEL_HINT_PROCESSING"))
				.localOrShuffleGrouping("downloadBolt", DownloadBolt.STREAM_LOCAL)
				.customGrouping("downloadBolt", new ShuffleGrouping());
		
		builder.setBolt("statsBolt", new StatsBolt(), 1)
				.globalGrouping("source", StatsInitTupleData.STREAM_ID)
				.globalGrouping("downloadBolt", StatsTupleData.STREAM_ID)
				.globalGrouping("processingBolt", StatsTupleData.STREAM_ID);
		
		if (isTest) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(topologyName, conf, builder.createTopology());
			Utils.sleep(600000);
			cluster.killTopology(topologyName);
			cluster.shutdown();
		} else {
			StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
		}
	}
}
