package eu.europeana.cloud.dps.topologies.media;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.dps.topologies.media.support.StatsTupleData;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;

public class StatsBolt extends BaseRichBolt {
	private static final Logger logger = LoggerFactory.getLogger(StatsBolt.class);
	
	private OutputCollector outputCollector;
	
	private long files = 0;
	private long size = 0;
	private long time = 0;
	private Map<String, Long> errors = new HashMap<>();
	
	
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
		
		String error = data.getError();
		if (StringUtils.isEmpty(error)) {
			size += data.getLength();
			time += data.getTime();
			files++;
		} else {
			Long counter = errors.getOrDefault(error, 0L);
			errors.put(error, counter + 1);
		}

		JSONObject json = new JSONObject();
		if (files > 0) {
			json.put("files", files);
			json.put("averageSize", size / files);
			json.put("averageSpeed", (size / time) * 1000);
			json.put("averageTime", time / files);
		}
		json.put("errors", errors);
		
		NotificationTuple nt = NotificationTuple.prepareUpdateTask(data.getTaskId(), json.toString(),
				TaskState.CURRENTLY_PROCESSING, new Date());
		
		outputCollector.emit(AbstractDpsBolt.NOTIFICATION_STREAM_NAME, nt.toStormTuple());
	}
}
