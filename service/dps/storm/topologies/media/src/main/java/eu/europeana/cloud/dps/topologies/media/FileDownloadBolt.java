package eu.europeana.cloud.dps.topologies.media;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.shade.org.apache.http.HttpEntity;
import org.apache.storm.shade.org.apache.http.HttpResponse;
import org.apache.storm.shade.org.apache.http.client.methods.HttpGet;
import org.apache.storm.shade.org.apache.http.impl.client.CloseableHttpClient;
import org.apache.storm.shade.org.apache.http.impl.client.HttpClients;
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
		if (StringUtils.isEmpty(resourceFile.error)) {
			parameters.put("length", Long.toString(resourceFile.bytes.length));
			parameters.put("time", Long.toString(resourceFile.time));
		} else {
			parameters.put("error", resourceFile.error);
		}
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
		public long time;
		public String error;
		
		public ResourceFile(String fileUrl) {
			try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
				long start = System.currentTimeMillis();
				HttpResponse response = httpclient.execute(new HttpGet(fileUrl));
				HttpEntity entity = response.getEntity();
				int status = response.getStatusLine().getStatusCode();
				if (status >= 200 && status < 300) {
					ByteArrayOutputStream baos = new ByteArrayOutputStream();
					IOUtils.copy(entity.getContent(), baos);
					
					time = System.currentTimeMillis() - start;
					bytes = baos.toByteArray();
					
					logger.debug("Downloaded file: {} (size: {}, time: {})", fileUrl, bytes.length, time);
				} else {
					error = "INVALID_STATUS_CODE: " + status;
				}
			} catch (IOException e) {
				logger.error("", e);
				error = e.getMessage();
			}
			
		}
	}
}
