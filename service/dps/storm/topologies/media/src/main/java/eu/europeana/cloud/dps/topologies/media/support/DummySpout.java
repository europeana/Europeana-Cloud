package eu.europeana.cloud.dps.topologies.media.support;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cglib.core.Constants;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.RepresentationIterator;

public class DummySpout extends BaseRichSpout implements Constants {
	
	private static final Logger logger = LoggerFactory.getLogger(DummySpout.class);
	
	private SpoutOutputCollector outputCollector;
	
	private RepresentationIterator representationIterator;
	
	private long emitLimit;
	private long emitCount = 0;
	private long startTime;
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		outputCollector = collector;
		
		DataSetServiceClient datasetClient = Util.getDataSetServiceClient(conf);
		String datasetProvider = (String) conf.get("MEDIATOPOLOGY_DATASET_PROVIDER");
		String datasetId = (String) conf.get("MEDIATOPOLOGY_DATASET_ID");
		representationIterator = datasetClient.getRepresentationIterator(datasetProvider, datasetId);
		if (!representationIterator.hasNext()) {
			throw new RuntimeException("There are no representations for dataset " + datasetId);
		}
		
		String limitKey = "MEDIATOPOLOGY_DATASET_EMIT_LIMIT";
		emitLimit = conf.containsKey(limitKey) ? (Long) conf.get(limitKey) : Long.MAX_VALUE;
	}
	
	@Override
	public void nextTuple() {
		if (startTime == 0)
			startTime = System.currentTimeMillis();
		while (representationIterator.hasNext() && emitCount < emitLimit) {
			Representation rep = representationIterator.next();
			if ("edm".equals(rep.getRepresentationName())) {
				final long taskId = 777;
				MediaTupleData data = new MediaTupleData(taskId, rep);
				outputCollector.emit(new Values(data));
				emitCount++;
				if (!(representationIterator.hasNext() && emitCount < emitLimit)) {
					outputCollector.emit(StatsInitTupleData.STREAM_ID,
							new Values(new StatsInitTupleData(taskId, startTime, emitCount)));
				}
				return;
			}
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
		declarer.declare(new Fields(MediaTupleData.FIELD_NAME));
		declarer.declareStream(StatsInitTupleData.STREAM_ID, new Fields(StatsInitTupleData.FIELD_NAME));
	}
}
