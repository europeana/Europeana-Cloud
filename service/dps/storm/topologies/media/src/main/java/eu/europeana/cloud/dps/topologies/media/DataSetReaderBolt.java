package eu.europeana.cloud.dps.topologies.media;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.RepresentationIterator;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.InputDataType;

public class DataSetReaderBolt extends BaseRichBolt {
	
	private static final Logger logger = LoggerFactory.getLogger(DataSetReaderBolt.class);
	
	private OutputCollector outputCollector;
	
	private DataSetServiceClient datasetClient;
	
	private int emitLimit;
	
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		outputCollector = collector;
		
		datasetClient = Util.getDataSetServiceClient(conf);
		
		String limitKey = "MEDIATOPOLOGY_DATASET_DATASET_LIMIT";
		emitLimit = conf.containsKey(limitKey) ? (int) conf.get(limitKey) : Integer.MAX_VALUE;
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(MediaTupleData.FIELD_NAME));
	}
	
	@Override
	public void execute(Tuple tuple) {
		ObjectMapper mapper = new ObjectMapper();
		DpsTask task;
		try {
			task = mapper.readValue(tuple.getString(0), DpsTask.class);
		} catch (IOException e) {
			logger.error("Message '{}' rejected because: {}", tuple.getString(0), e.getMessage());
			logger.debug("Exception details", e);
			outputCollector.ack(tuple);
			return;
		}
		logger.debug("Task {} parsed", task.getTaskName());
		
		String datasetUrl = task.getDataEntry(InputDataType.DATASET_URLS).get(0);
		try {
			UrlParser urlParser = new UrlParser(datasetUrl);
			RepresentationIterator repIterator = datasetClient.getRepresentationIterator(
					urlParser.getPart(UrlPart.DATA_PROVIDERS), urlParser.getPart(UrlPart.DATA_SETS));
			int limit = emitLimit;
			while (repIterator.hasNext() && limit-- > 0) {
				Representation rep = repIterator.next();
				if ("edm".equals(rep.getRepresentationName())) {
					MediaTupleData data = new MediaTupleData(task.getTaskId());
					data.setEdmRepresentation(rep);
					outputCollector.emit(new Values(data));
				}
			}
		} catch (MalformedURLException e) {
			logger.error("Url problem: " + datasetUrl, e);
		}
		outputCollector.ack(tuple);
	}
}
