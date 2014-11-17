package eu.europeana.cloud.service.dps.storm.bolts;

import java.io.IOException;
import java.util.ArrayList;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.europeana.cloud.service.dps.kafka.message.DPSMessage;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ProcessDPSInputMessageBolt extends BaseBasicBolt {
	public static final Logger LOG = LoggerFactory.getLogger(ProcessDPSInputMessageBolt.class);

	private static final long serialVersionUID = 1L;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		ObjectMapper mapper = new ObjectMapper();
		DPSMessage message = null;
		try {
			message = mapper.readValue(tuple.getString(0), DPSMessage.class);
		} catch (IOException e) {
			e.printStackTrace();
		}
		// LOG.info(message.getDataEntry());

		// if message exists...
		if (message != null) {
			ArrayList<String> data = (ArrayList<String>) message.getData();
			ArrayList<String> attributes = (ArrayList<String>) message.getAttributes();

			if (data.size() == attributes.size()) {
				for (int i = 0; i < data.size(); i++) {
					collector.emit(new Values(data.get(i), attributes.get(i)));
					LOG.info("DATO " + data.get(i));
					LOG.info("ATTRIBUTO " + attributes.get(i));
				}
			}
		}
	}
}
