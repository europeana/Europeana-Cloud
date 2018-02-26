package eu.europeana.cloud.dps.topologies.media.support;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.InputDataType;

public class DummySpout extends BaseRichSpout {
	
	private static final Logger logger = LoggerFactory.getLogger(DummySpout.class);
	
	private SpoutOutputCollector outputCollector;
	
	private DpsTask task;
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		outputCollector = collector;
		
		task = new DpsTask();
		logger.info("Created dummy task: " + task.getTaskId());
		task.setTaskName("test777");
		String serviceUrl = (String) conf.get("MEDIATOPOLOGY_FILE_SERVICE_URL");
		String datasetProvider = (String) conf.get("MEDIATOPOLOGY_DATASET_PROVIDER");
		String datasetId = (String) conf.get("MEDIATOPOLOGY_DATASET_ID");
		task.addDataEntry(InputDataType.DATASET_URLS, Arrays.asList(
				serviceUrl + "/data-providers/" + datasetProvider + "/data-sets/" + datasetId));
		
	}
	
	@Override
	public void nextTuple() {
		if (task != null) {
			try {
				String json = new ObjectMapper().writeValueAsString(task);
				json = json.replaceFirst("\"taskId\":[-0-9]+", "\"taskId\":777");
				outputCollector.emit(new Values(json), "test1");
			} catch (IOException e) {
				logger.error("task serialization problem", e);
			}
			task = null;
			return;
		}
		// everything retrieved
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("task-json"));
	}
}
