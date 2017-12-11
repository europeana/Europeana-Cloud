package eu.europeana.cloud.dps.topologies.media;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;

public class NumberSpout extends BaseRichSpout {

	private static final Logger logger = LoggerFactory.getLogger(NumberSpout.class);

	private SpoutOutputCollector collector;
	int count = 0;

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		if (count < 50) {
			count++;
			Map<String, String> parameters = new HashMap<>();
			parameters.put("number", Integer.toString(count));
			collector.emit(Arrays.<Object>asList(777L, "", "", null,
					parameters, new Revision(), null), count);
		} else {
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(StormTaskTuple.getFields());
	}

	@Override
	public void ack(Object msgId) {
		logger.info("Spout got ack for " + msgId);
	}
}
