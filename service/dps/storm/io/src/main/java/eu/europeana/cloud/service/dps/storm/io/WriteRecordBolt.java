package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.service.dps.storm.StormTask;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.service.dps.DpsKeys;

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
	public void execute(StormTask t) {

		try {

			final String record = t.getFileByteData();
			final String recordEdited = record + " edited by Storm!";
			InputStream contentStream = new ByteArrayInputStream(
					recordEdited.getBytes());

			final String fileUrl = t.getParameter(DpsKeys.OUTPUT_URL);
			URI uri = null;

			uri = mcsClient.modifyFile(fileUrl, contentStream, mediaType);
			LOGGER.info("file modified, new URI:" + uri);

		} catch (Exception e) {
			LOGGER.error(e.getMessage());
		}
	}
}
