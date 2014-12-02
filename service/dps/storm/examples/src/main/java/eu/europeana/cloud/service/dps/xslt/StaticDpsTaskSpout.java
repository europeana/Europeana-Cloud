package eu.europeana.cloud.service.dps.xslt;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.kafka.message.StormTask;
import eu.europeana.cloud.service.dps.kafka.message.StormTupleKeys;

/**
 * Always uses the same DpsTask (instead of consuming tasks from a Kafka Topic)
 * 
 * Useful for testing without having Kafka deployed.
 */
public class StaticDpsTaskSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;

	/** The task to be consumed */
	private DpsTask task;

	public StaticDpsTaskSpout(DpsTask task) {
		this.task = task;
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {

		this.collector = collector;
	}

	@Override
	public void nextTuple() {

		try {
			Utils.sleep(50);

			HashMap<String, String> taskParameters = task.getParameters();
			System.out.println("taskParameters size=" + taskParameters.size());
			
			List<String> files = task.getDataEntry(DpsTask.FILE_URLS);
			System.out.println("files size=" + files.size());
			
			for (String file : files) {
				System.out.println("emmiting..." + file);
				collector.emit(new StormTask(file, taskParameters).toStormTuple());
			}
			
		} catch (Exception e) {
			System.out.println("StaticDpsTaskSpout error:" + e.getMessage());
		}
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(
				StormTupleKeys.INPUT_FILES_TUPLE_KEY,
					StormTupleKeys.FILE_CONTENT_TUPLE_KEY,
						StormTupleKeys.PARAMETERS_TUPLE_KEY));
	}
}