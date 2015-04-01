package eu.europeana.cloud.service.dps.storm.textstripping;

import java.io.IOException;
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
import backtype.storm.tuple.Values;
import eu.europeana.cloud.service.dps.DpsTask;

public class KafkaParseTextStrippingTaskBolt extends BaseBasicBolt {

    public static final Logger LOGGER = LoggerFactory
            .getLogger(KafkaParseTextStrippingTaskBolt.class);

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(
                "providerId", "datasetId"));
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
        HashMap<String, List<String>> taskInputData = task.getInputData();

        String providerId = taskInputData.get("providerId").get(0);
        String datasetId = taskInputData.get("datasetId").get(0);

        collector.emit(new Values(providerId, datasetId));

    }
}
