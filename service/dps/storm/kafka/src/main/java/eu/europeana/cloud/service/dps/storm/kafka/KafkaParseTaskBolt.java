package eu.europeana.cloud.service.dps.storm.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.storm.StormTupleKeys;

public class KafkaParseTaskBolt extends BaseBasicBolt {

	public static final Logger LOGGER = LoggerFactory
			.getLogger(KafkaParseTaskBolt.class);

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
			LOGGER.info("emitting..." + fileUrl);
			List<Object> prova = new ArrayList<Object>();
			prova.add(fileUrl);
			prova.add("");
			prova.add(taskParameters);
			collector.emit(prova);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(StormTupleKeys.INPUT_FILES_TUPLE_KEY,
				StormTupleKeys.FILE_CONTENT_TUPLE_KEY,
				StormTupleKeys.PARAMETERS_TUPLE_KEY));
	}
}