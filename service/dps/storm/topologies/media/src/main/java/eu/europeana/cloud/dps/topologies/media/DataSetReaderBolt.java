package eu.europeana.cloud.dps.topologies.media;

import java.io.IOException;
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
import eu.europeana.cloud.dps.topologies.media.support.MediaTupleData;
import eu.europeana.cloud.dps.topologies.media.support.StatsInitTupleData;
import eu.europeana.cloud.dps.topologies.media.support.Util;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.RepresentationIterator;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.InputDataType;

public class DataSetReaderBolt extends BaseRichBolt {
	
	private static final Logger logger = LoggerFactory.getLogger(DataSetReaderBolt.class);
	
	private OutputCollector outputCollector;
	
	private DataSetServiceClient datasetClient;
	
	private long emitLimit;
	
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		outputCollector = collector;
		
		datasetClient = Util.getDataSetServiceClient(conf);
		
		String limitKey = "MEDIATOPOLOGY_DATASET_EMIT_LIMIT";
		emitLimit = conf.containsKey(limitKey) ? (Long) conf.get(limitKey) : Long.MAX_VALUE;
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(MediaTupleData.FIELD_NAME));
		declarer.declareStream(StatsInitTupleData.STREAM_ID, new Fields(StatsInitTupleData.FIELD_NAME));
	}
	
	@Override
	public void execute(Tuple tuple) {
		DpsTask task;
		UrlParser datasetUrlParser;
		try {
			task = new ObjectMapper().readValue(tuple.getString(0), DpsTask.class);
			String datasetUrl = task.getDataEntry(InputDataType.DATASET_URLS).get(0);
			datasetUrlParser = new UrlParser(datasetUrl);
		} catch (IOException e) {
			logger.error("Message '{}' rejected because: {}", tuple.getString(0), e.getMessage());
			logger.debug("Exception details", e);
			outputCollector.ack(tuple);
			return;
		}
		logger.debug("Task {} parsed", task.getTaskName());
		
		try {
			long start = System.currentTimeMillis();
			long count = 0;
			
			RepresentationIterator repIterator = datasetClient.getRepresentationIterator(
					datasetUrlParser.getPart(UrlPart.DATA_PROVIDERS), datasetUrlParser.getPart(UrlPart.DATA_SETS));
			while (repIterator.hasNext() && count < emitLimit) {
				Representation rep = repIterator.next();
				if ("edm".equals(rep.getRepresentationName())) {
					MediaTupleData data = new MediaTupleData(task.getTaskId());
					data.setEdmRepresentation(rep);
					outputCollector.emit(new Values(data));
					count++;
				}
			}
			logger.info("Task {}: emitted {} EDMs in {} ms", task.getTaskName(), count,
					System.currentTimeMillis() - start);
			outputCollector.emit(StatsInitTupleData.STREAM_ID,
					new Values(new StatsInitTupleData(task.getTaskId(), start, count)));
			outputCollector.ack(tuple);
		} catch (DriverException e) {
			logger.error("MCS error", e);
			outputCollector.fail(tuple);
		}
	}
}
