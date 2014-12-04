package eu.europeana.cloud.service.dps.xslt;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

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