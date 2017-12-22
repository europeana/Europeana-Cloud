package eu.europeana.cloud.dps.topologies.media;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;

import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;

public class DummyTaskSpout extends BaseRichSpout {
	
	private SpoutOutputCollector collector;
	private boolean alreadyEmitted = false;
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}
	
	@Override
	public void nextTuple() {
		if (!alreadyEmitted) {
			Map<String, String> parameters = new HashMap<>();
			parameters.put(PluginParameterKeys.DATASET_URL,
					"https://test-cloud.europeana.eu/api/data-providers/mms_prov/data-sets/mms_set");
			parameters.put(PluginParameterKeys.AUTHORIZATION_HEADER, "mms_user:pass"); // TODO dobry sposób zapisu?
																						// +uzupełnić hasło
			Revision revision = new Revision(); // TODO skonsultować z Olą?
			StormTaskTuple tuple = new StormTaskTuple(777L, "dummy-media-task-1", null, null, parameters, revision);
			collector.emit(tuple.toStormTuple());
			alreadyEmitted = true;
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(StormTaskTuple.getFields());
	}
	
}
