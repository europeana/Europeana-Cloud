package eu.europeana.cloud.dps.topologies.media;

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
	}
	
	@Override
	public void nextTuple() {
		while (representationIterator.hasNext()) {
			Representation rep = representationIterator.next();
			if ("edm".equals(rep.getRepresentationName())) {
				MediaTupleData data = new MediaTupleData(777L); // TODO
				data.setEdmRepresentation(rep);
				outputCollector.emit(new Values(data));
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
	}
}
