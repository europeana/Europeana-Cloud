package eu.europeana.cloud.service.dps.storm;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import eu.europeana.cloud.service.dps.PluginParameterKeys;

/**
 * Always uses the same DpsTask (instead of consuming tasks from a Kafka Topic)
 * 
 * Useful for testing without having Kafka deployed.
 */
public class DpsTaskSpoutTest extends BaseRichSpout {

	private SpoutOutputCollector collector;

	private static final Logger LOGGER = LoggerFactory
			.getLogger(DpsTaskSpoutTest.class);

	private final String fileUrl = "http://heliopsis.man.poznan.pl/mcs/records"
			+ "/FJ0DL14CDQB/representations/oai-pmh/versions/c68eccb0-b7ad-11e4-93c4-00505682006e/files/46b0ab4b-78f2-45e3-aa97-658efff60e19";

	public DpsTaskSpoutTest() {
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {

		this.collector = collector;
	}

	@Override
	public void nextTuple() {

		try {
			Utils.sleep(50);

			HashMap<String, String> taskParameters = new HashMap<String, String>();
			taskParameters.put(PluginParameterKeys.XSLT_URL,
					"http://ecloud.eanadev.org:8080/hera/sample_xslt.xslt");
			taskParameters
					.put(PluginParameterKeys.OUTPUT_URL,
							"http://heliopsis.man.poznan.pl/mcs/records/FJ0DL14CDQB/representations/oai-pmh/versions/c68eccb0-b7ad-11e4-93c4-00505682006e/files/46b0ab4b-78f2-45e3-aa97-658efff60e19.OUT");
			LOGGER.info("emitting : " + fileUrl);
			collector.emit(new StormTask(fileUrl, "", taskParameters)
					.toStormTuple());

		} catch (Exception e) {
			System.out.println("DpsTaskSpoutTest error:" + e.getMessage());
		}
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(StormTupleKeys.INPUT_FILES_TUPLE_KEY,
				StormTupleKeys.FILE_CONTENT_TUPLE_KEY,
				StormTupleKeys.PARAMETERS_TUPLE_KEY));
	}
}