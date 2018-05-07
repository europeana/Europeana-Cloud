package eu.europeana.cloud.dps.topologies.media;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.client.methods.HttpAsyncMethods;
import org.apache.http.nio.client.methods.ZeroCopyConsumer;
import org.apache.http.nio.protocol.HttpAsyncRequestProducer;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.europeana.cloud.dps.topologies.media.support.MediaTupleData;
import eu.europeana.cloud.dps.topologies.media.support.MediaTupleData.FileInfo;
import eu.europeana.cloud.dps.topologies.media.support.StatsTupleData;
import eu.europeana.cloud.dps.topologies.media.support.StatsTupleData.Status;
import eu.europeana.cloud.dps.topologies.media.support.TempFileSync;
import eu.europeana.metis.mediaservice.MediaProcessor;

public class DownloadBolt extends HttpClientBolt {
	
	private static final Logger logger = LoggerFactory.getLogger(DownloadBolt.class);
	
	/**
	 * Tuple stream dedicated for records with large downloaded resources. Goes to
	 * bolts on the same machine to save network traffic.
	 * @see #localStreamThreshold
	 */
	public static final String STREAM_LOCAL = "download-local";
	
	private static final File ERROR_FLAG = new File("a");
	
	private transient RequestConfig requestConfig;
	
	private InetAddress localAddress;
	
	private long localStreamThreshold;
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(MediaTupleData.FIELD_NAME, StatsTupleData.FIELD_NAME));
		declarer.declareStream(STREAM_LOCAL, new Fields(MediaTupleData.FIELD_NAME, StatsTupleData.FIELD_NAME));
		declarer.declareStream(StatsTupleData.STREAM_ID, new Fields(StatsTupleData.FIELD_NAME));
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		localAddress = TempFileSync.startServer(stormConf);
		localStreamThreshold =
				(long) stormConf.getOrDefault("MEDIATOPOLOGY_FILE_TRANSFER_THRESHOLD_MB", 10) * 1024 * 1024;
		requestConfig = RequestConfig.custom()
				.setMaxRedirects(FOLLOW_REDIRECTS)
				.setConnectTimeout(10000)
				.setSocketTimeout(20000)
				.build();
	}
	
	@Override
	protected HttpAsyncRequestProducer createRequestProducer(FileInfo fileInfo) {
		HttpGet request = new HttpGet(fileInfo.getUrl());
		request.setConfig(requestConfig);
		return HttpAsyncMethods.create(request);
	}
	
	@Override
	protected ZeroCopyConsumer<Void> createResponseConsumer(FileInfo fileInfo, StatsTupleData stats, Tuple tuple)
			throws IOException {
		File content = File.createTempFile("media", null);
		return new DownloadConsumer(fileInfo, content, tuple, stats);
	}

	private final class DownloadConsumer extends ZeroCopyConsumer<Void> {
		private final StatsTupleData stats;
		private final FileInfo fileInfo;
		private final File content;
		private final Tuple tuple;
		
		private DownloadConsumer(FileInfo fileInfo, File content, Tuple tuple, StatsTupleData stats)
				throws FileNotFoundException {
			super(content);
			this.stats = stats;
			this.fileInfo = fileInfo;
			this.content = content;
			this.tuple = tuple;
		}
		
		@Override
		protected void onResponseReceived(HttpResponse response) {
			String mimeType = ContentType.get(response.getEntity()).getMimeType();
			if (MediaProcessor.supportsLinkProcessing(mimeType)) {
				logger.debug("Skipping download: {}", fileInfo.getUrl());
				cleanup();
				fileInfo.setMimeType(mimeType);
				statusUpdate(Status.STATUS_OK);
				cancel();
				return;
			}
			super.onResponseReceived(response);
		}
		
		@Override
		protected Void process(HttpResponse response, File file, ContentType contentType) throws Exception {
			int status = response.getStatusLine().getStatusCode();
			long byteCount = file.length();
			if (status < 200 || status >= 300 || byteCount == 0) {
				logger.info("Download error (code {}) for {}", status, fileInfo.getUrl());
				cleanup();
				fileInfo.setContent(ERROR_FLAG);
				statusUpdate("DOWNLOAD: STATUS CODE " + status);
				return null;
			}
			fileInfo.setContent(file);
			fileInfo.setMimeType(contentType.getMimeType());
			fileInfo.setContentSource(localAddress);
			
			synchronized (stats) {
				long now = System.currentTimeMillis();
				stats.setDownloadEndTime(now);
				if (stats.getDownloadStartTime() == 0) {
					// too hard to determine actual start time, not significant for global measure
					stats.setDownloadStartTime(now);
				}
				stats.setDownloadedBytes(stats.getDownloadedBytes() + byteCount);
			}
			
			logger.debug("Downloaded {} bytes: {}", byteCount, fileInfo.getUrl());
			statusUpdate(Status.STATUS_OK);
			return null;
		}
		
		@Override
		protected void releaseResources() {
			super.releaseResources();
			Exception e = getException();
			if (e != null) {
				String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
				logger.info("Download exception ({}) for {}", msg, fileInfo.getUrl());
				logger.trace("Download failure details:", e);
				cleanup();
				fileInfo.setContent(ERROR_FLAG);
				statusUpdate("CONNECTION ERROR: " + msg);
			}
		}
		
		private void statusUpdate(String status) {
			synchronized (stats) {
				stats.addStatus(fileInfo.getUrl(), status);
				if (stats.getStatuses().size() == stats.getResourceCount()) {
					boolean someSuccess = stats.getErrors().size() < stats.getResourceCount();
					if (someSuccess) {
						MediaTupleData data = (MediaTupleData) tuple.getValueByField(MediaTupleData.FIELD_NAME);
						data.getFileInfos().removeIf(f -> f.getContent() == ERROR_FLAG);
						if (stats.getDownloadedBytes() < localStreamThreshold) {
							outputCollector.emit(tuple, new Values(data, stats));
						} else {
							outputCollector.emit(STREAM_LOCAL, tuple, new Values(data, stats));
						}
					} else {
						outputCollector.emit(StatsTupleData.STREAM_ID, tuple, new Values(stats));
					}
					outputCollector.ack(tuple);
				}
			}
		}
		
		private void cleanup() {
			if (content.exists()) {
				try {
					Files.delete(content.toPath());
				} catch (IOException e) {
					logger.warn("Could not remove temp file " + content, e);
				}
			}
		}
	}
	
}
