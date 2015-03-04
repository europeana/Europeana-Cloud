package eu.europeana.cloud.service.dps.xslt.kafka.bolts;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;

public class KafkaParseTaskBolt extends BaseBasicBolt {
	
	public static final Logger LOG = LoggerFactory.getLogger(KafkaParseTaskBolt.class);

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		ObjectMapper mapper = new ObjectMapper();
		DpsTask task = null;
		try {
			task = mapper.readValue(tuple.getString(0), DpsTask.class);
		} catch (IOException e) {
			e.printStackTrace();
		}

		HashMap<String, String> taskParameters = task.getParameters();
		System.out.println("taskParameters size=" + taskParameters.size());

		List<String> files = task.getDataEntry(DpsTask.FILE_URLS);
		System.out.println("files size=" + files.size());

		for (String fileUrl : files) {
			System.out.println("emmiting..." + fileUrl);
			collector.emit(new StormTaskTuple(task.getTaskId(), task.getTaskName(), fileUrl, "", taskParameters).toStormTuple());
		}
	}
}
