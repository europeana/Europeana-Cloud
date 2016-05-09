package eu.europeana.cloud.service.dps.examples;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.StormTupleKeys;

/**
 * Always uses the same {@link DpsTask} (instead of consuming tasks from a Kafka Topic)
 * 
 * Useful for testing without having Kafka deployed.
 * 
 * @author manos
 */
public class StaticDpsTaskSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(StaticDpsTaskSpout.class);

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
			Map<String, String> taskParameters = task.getParameters();
			LOGGER.info("taskParameters size=" + taskParameters.size());
			
			List<String> files = task.getDataEntry(DpsTask.FILE_URLS);
			LOGGER.info("files size=" + files.size());
			
			for (String fileUrl : files) {
				LOGGER.info("emmiting..." + fileUrl);
				collector.emit(new StormTaskTuple(task.getTaskId(), task.getTaskName(),  fileUrl, null, taskParameters).toStormTuple());
			}
			
			Utils.sleep(6000000);

		} catch (Exception e) {
			LOGGER.error("StaticDpsTaskSpout error:" + e.getMessage());
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
				StormTupleKeys.TASK_ID_TUPLE_KEY,
				StormTupleKeys.TASK_NAME_TUPLE_KEY,
				StormTupleKeys.INPUT_FILES_TUPLE_KEY,
					StormTupleKeys.FILE_CONTENT_TUPLE_KEY,
						StormTupleKeys.PARAMETERS_TUPLE_KEY));
	}
}