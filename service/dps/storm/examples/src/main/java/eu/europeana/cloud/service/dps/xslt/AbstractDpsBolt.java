package eu.europeana.cloud.service.dps.xslt;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.service.dps.kafka.message.DpsKeys;
import eu.europeana.cloud.service.dps.kafka.message.DpsProcessingParams;
import eu.europeana.cloud.service.dps.kafka.message.StormTask;
import eu.europeana.cloud.service.dps.kafka.message.StormTupleKeys;

/**
 */
public abstract class AbstractDpsBolt extends BaseRichBolt {

	public abstract void execute(StormTask t) ;
	
	@Override
	public void execute(Tuple tuple) {

		try {
			StormTask t = StormTask.fromStormTuple(tuple);
			execute(t);

		} catch (Exception e) {
			System.out.println("AbstractDpsBolt error:" + e.getMessage());
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(
				StormTupleKeys.INPUT_FILES_TUPLE_KEY,
					StormTupleKeys.FILE_CONTENT_TUPLE_KEY,
						StormTupleKeys.PARAMETERS_TUPLE_KEY));
	}
}