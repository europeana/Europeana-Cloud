package eu.europeana.cloud.service.dps.storm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public abstract class AbstractDpsBolt extends BaseRichBolt {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDpsBolt.class);

    public abstract void execute(StormTaskTuple t);

    @Override
    public void execute(Tuple tuple) {

        try {
            StormTaskTuple t = StormTaskTuple.fromStormTuple(tuple);
            execute(t);

        } catch (Exception e) {
        	LOGGER.error("AbstractDpsBolt error:" + e.getMessage());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(
                StormTupleKeys.TASK_ID_TUPLE_KEY,
                StormTupleKeys.TASK_NAME_TUPLE_KEY,
                StormTupleKeys.INPUT_FILES_TUPLE_KEY,
                StormTupleKeys.FILE_CONTENT_TUPLE_KEY,
                StormTupleKeys.PARAMETERS_TUPLE_KEY));
    }
}
