package eu.europeana.cloud.service.dps.storm.io;

import java.io.InputStream;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.metric.api.CountMetric;
import backtype.storm.metric.api.MeanReducer;
import backtype.storm.metric.api.MultiCountMetric;
import backtype.storm.metric.api.ReducedMetric;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.utils.Utils;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.PersistentCountMetric;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.ZookeeperMultiCountMetric;

/**
 */
public class ReadFileBolt extends AbstractDpsBolt {

	private OutputCollector collector;

	private static final Logger LOGGER = LoggerFactory
			.getLogger(ReadFileBolt.class);

	/** Properties to connect to eCloud */
	private String zkAddress;
	private String ecloudMcsAddress;
	private String username;
	private String password;
	
	private transient CountMetric countMetric;
	private transient PersistentCountMetric pCountMetric;
	private transient MultiCountMetric wordCountMetric;
	private transient ReducedMetric wordLengthMeanMetric;
	private transient ZookeeperMultiCountMetric zMetric;
	
	private FileServiceClient fileClient;

	public ReadFileBolt(String zkAddress, String ecloudMcsAddress, String username,
			String password) {

		this.zkAddress = zkAddress;
		this.ecloudMcsAddress = ecloudMcsAddress;
		this.username = username;
		this.password = password;
	}

	@Override
	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {

		this.collector = collector;
		fileClient = new FileServiceClient(ecloudMcsAddress, username, password);

		initMetrics(context);
	}

	void initMetrics(TopologyContext context) {

		countMetric = new CountMetric();
		pCountMetric = new PersistentCountMetric();
		wordCountMetric = new MultiCountMetric();
		wordLengthMeanMetric = new ReducedMetric(new MeanReducer());
		zMetric = new ZookeeperMultiCountMetric(zkAddress);
		
		context.registerMetric("read_records=>", countMetric, 1);
		context.registerMetric("pCountMetric_records=>", pCountMetric, 1);
		context.registerMetric("word_count=>", wordCountMetric, 1);
		context.registerMetric("word_length=>", wordLengthMeanMetric, 1);
		context.registerMetric("zMetric=>", zMetric, 1);
	}

	@Override
	public void execute(StormTaskTuple t) {

		String fileData = null;
		String fileUrl = null;

		try {

			fileUrl = t.getFileUrl();

			LOGGER.info("getTaskId: {}", t.getTaskId());
			LOGGER.info("logger fetching file: {}", fileUrl);
			LOGGER.debug("fetching file: " + fileUrl);
			
			fileData = getFileContentAsString(fileUrl);

			updateMetrics(t, fileData);

		} catch (Exception e) {
			LOGGER.error("ReadFileBolt error:" + e.getMessage());
		}
		
		t.setFileUrl(fileUrl);
		t.setFileData(fileData);
		
		Utils.sleep(100);
		collector.emit(t.toStormTuple());
	}

	void updateMetrics(StormTaskTuple t, String word) {
		
		countMetric.incr();
		pCountMetric.incr();
		wordCountMetric.scope(word).incr();
		wordLengthMeanMetric.update(word.length());
		zMetric.incr(t.getTaskId());
		LOGGER.info("ReadFileBolt: metrics updated");
	}

	private String getFileContentAsString(String fileUrl) {
		try {
			InputStream stream = fileClient.getFile(fileUrl);
			return IOUtils.toString(stream);
		} catch (Exception e) {
			LOGGER.error("ReadFileBolt error:" + e.getMessage());
		}
		return null;
	}
}