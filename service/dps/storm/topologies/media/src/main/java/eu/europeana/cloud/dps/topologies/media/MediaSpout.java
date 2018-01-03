package eu.europeana.cloud.dps.topologies.media;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.stream.Collectors;

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
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.service.mcs.exception.MCSException;

public class MediaSpout extends BaseRichSpout implements Constants {
	
	private static final Logger logger = LoggerFactory.getLogger(MediaSpout.class);
	
	private SpoutOutputCollector outputCollector;
	
	private DataSetServiceClient datasetClient;
	private ArrayDeque<Representation> currentSliceResults = new ArrayDeque<>();
	private String datasetProvider, datasetId;
	private String nextSliceId;
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		outputCollector = collector;
		
		Map<String, String> config = conf;
		datasetClient = Util.getDataSetServiceClient(config);
		datasetProvider = config.get("MEDIATOPOLOGY_DATASET_PROVIDER");
		datasetId = config.get("MEDIATOPOLOGY_DATASET_ID");
		retrieveSlice();
	}
	
	@Override
	public void nextTuple() {
		while (currentSliceResults.isEmpty()) {
			if (nextSliceId != null) {
				retrieveSlice();
			} else {
				// everything retrieved
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
				return;
			}
		}
		
		MediaTupleData data = new MediaTupleData(777L);
		data.setEdmRepresentation(currentSliceResults.remove());
		outputCollector.emit(new Values(data));
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(MediaTupleData.FIELD_NAME));
	}
	
	private void retrieveSlice() {
		try {
			long start = System.currentTimeMillis();
			ResultSlice<Representation> data =
					datasetClient.getDataSetRepresentationsChunk(datasetProvider, datasetId, nextSliceId);
			logger.debug("dataSet slice downloaded in {} ms", System.currentTimeMillis() - start);
			currentSliceResults.addAll(data.getResults().stream()
					.filter(r -> "edm".equals(r.getRepresentationName()))
					.collect(Collectors.toList()));
			nextSliceId = data.getNextSlice();
		} catch (MCSException e) {
			throw new RuntimeException("File service connection error", e);
		}
	}
	
}
