package eu.europeana.cloud.service.dps.xslt;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.service.dps.kafka.message.DpsKeys;
import eu.europeana.cloud.service.dps.kafka.message.StormTask;

/**
 */
public class ReadFileBolt extends AbstractDpsBolt {

	private OutputCollector collector;
	
    private static final Logger LOGGER = LoggerFactory.getLogger(ReadFileBolt.class);

	/** Properties to connect to eCloud */
	private String ecloudMcsAddress;
	private String username;
	private String password;

	private FileServiceClient fileClient;

	public ReadFileBolt(String ecloudMcsAddress, String username,
			String password) {

		this.ecloudMcsAddress = ecloudMcsAddress;
		this.username = username;
		this.password = password;
	}

	@Override
	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {

		this.collector = collector;
		fileClient = new FileServiceClient(ecloudMcsAddress, username, password);
	}


	@Override
	public void execute(StormTask t) {

		String file = null;
		String fileUrl = null;
		String xsltUrl = null;

		try {
			
			fileUrl = t.getFileUrl();
			xsltUrl = t.getParameter(DpsKeys.XSLT_URL);

			LOGGER.info("logger fetching file: {}", fileUrl);
			System.out.println("fetching file: " + fileUrl);
			file = getFileContent(fileUrl);
			System.out.println("emmiting file:" + file);

		} catch (Exception e) {
			System.out.println("ReadFileBolt error:" + e.getMessage());
		}

		Utils.sleep(100);
		collector.emit(new Values(fileUrl, file, xsltUrl));
	}

	private String getFileContent(String fileUrl) {

		try {
			String file = fileClient.getFileAsString(fileUrl);
			return file;
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		return null;
	}
}