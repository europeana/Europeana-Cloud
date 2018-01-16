package eu.europeana.cloud.dps.topologies.media;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.europeana.cloud.dps.topologies.media.support.StatsTupleData;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;

public class StatsBolt extends BaseRichBolt {
	private static final Logger logger = LoggerFactory.getLogger(StatsBolt.class);
	
	private OutputCollector outputCollector;
	
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.outputCollector = collector;
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(AbstractDpsBolt.NOTIFICATION_STREAM_NAME, NotificationTuple.getFields());
	}
	
	@Override
	public void execute(Tuple tuple) {
		StatsTupleData data = (StatsTupleData) tuple.getValueByField(StatsTupleData.FIELD_NAME);
		logger.trace("Stats bolt executing for task " + data.getTaskId());
	}
}
