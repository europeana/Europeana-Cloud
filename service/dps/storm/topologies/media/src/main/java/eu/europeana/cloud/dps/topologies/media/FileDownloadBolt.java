package eu.europeana.cloud.dps.topologies.media;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.io.IOUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;

public class FileDownloadBolt extends BaseRichBolt implements TupleConstants {
	
	private OutputCollector outputCollector;
	
	private static final Logger logger = LoggerFactory.getLogger(FileDownloadBolt.class);
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		outputCollector = collector;
		
	}
	
	@Override
	public void execute(Tuple input) {
		String fileUrl = input.getStringByField(URL);

		ResourceFile resourceFile = new ResourceFile(fileUrl);

		Map<String, String> parameters = new HashMap<>();
		parameters.put("length", Long.toString(resourceFile.bytes.length));
		parameters.put("time", Long.toString(resourceFile.downloadTime));
		parameters.put("speed", Long.toString(resourceFile.downloadTime));
		Revision revision = new Revision();
		StormTaskTuple tuple =
				new StormTaskTuple(777L, "dummy-media-task-", null, null,
						parameters, revision);
		
		outputCollector.emit(tuple.toStormTuple());
		
		try {
			Random generator = new Random();
			int i = generator.nextInt(100) + 1;
			Thread.sleep(i);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(StormTaskTuple.getFields());
		
	}
	
	private class ResourceFile {
		public byte[] bytes;
		public long downloadTime;
		public long speed;
		
		public ResourceFile(String fileUrl) {
			logger.info("Downloading file " + fileUrl);
			try {
				URL url = new URL(fileUrl);
				URLConnection conn = url.openConnection();
				conn.setConnectTimeout(5000);
				conn.setReadTimeout(5000);
				conn.connect();
				
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				long start = System.currentTimeMillis();
				IOUtils.copy(conn.getInputStream(), baos);
				long end = System.currentTimeMillis();
				
				downloadTime = end - start;
				bytes = baos.toByteArray();
				speed = bytes.length / downloadTime;
			} catch (IOException e) {
				logger.error("", e);
			}
			logger.info("Downloaded file: " + fileUrl + "(size:  " + bytes.length + " )");
		}
	}
}
