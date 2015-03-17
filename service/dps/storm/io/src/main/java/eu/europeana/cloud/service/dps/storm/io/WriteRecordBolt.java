package eu.europeana.cloud.service.dps.storm.io;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;

/**
 * Stores a Record on the cloud.
 * 
 * Receives a byte array representing a Record from a tuple, creates and stores
 * a new Record on the cloud, and emits the URL of the newly created record.
 */
public class WriteRecordBolt extends AbstractDpsBolt {

	private OutputCollector collector;

	private String ecloudMcsAddress;
	private String username;
	private String password;

	private FileServiceClient mcsClient;

	private static final String mediaType = "text/plain";
	
	public static final Logger LOGGER = LoggerFactory.getLogger(WriteRecordBolt.class);

	public WriteRecordBolt(String ecloudMcsAddress, String username,
			String password) {

		this.ecloudMcsAddress = ecloudMcsAddress;
		this.username = username;
		this.password = password;
	}

	@Override
	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {

		mcsClient = new FileServiceClient(ecloudMcsAddress, username, password);
		this.collector = collector;
	}

	@Override
	public void execute(StormTaskTuple t) {

		try {
			LOGGER.info("WriteRecordBolt: persisting...");

			final String record = t.getFileByteData();
			String outputUrl = t.getParameter(PluginParameterKeys.OUTPUT_URL);
			
			if (outputUrl == null) {
				// in case OUTPUT_URL is not provided use a random one, using the input URL as the base 
				outputUrl = t.getFileUrl();
				outputUrl = StringUtils.substringBefore(outputUrl, "/files");
				
				LOGGER.info("WriteRecordBolt: OUTPUT_URL is not provided");
			}
			LOGGER.info("WriteRecordBolt: OUTPUT_URL: {}", outputUrl);
			
			URI uri = null;
			uri = mcsClient.uploadFile(outputUrl, new ByteArrayInputStream(record.getBytes()), mediaType);
			
			LOGGER.info("WriteRecordBolt: file modified, new URI:" + uri);
			
			collector.emit(t.toStormTuple());

		} catch (Exception e) {
			LOGGER.error(e.getMessage());
		}
	}
}
