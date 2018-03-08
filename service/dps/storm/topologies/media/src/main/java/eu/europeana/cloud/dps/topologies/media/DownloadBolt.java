package eu.europeana.cloud.dps.topologies.media;

import java.io.IOException;

import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.client.methods.HttpAsyncMethods;
import org.apache.http.nio.client.methods.ZeroCopyConsumer;
import org.apache.http.nio.protocol.HttpAsyncRequestProducer;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.europeana.cloud.dps.topologies.media.support.MediaTupleData;
import eu.europeana.cloud.dps.topologies.media.support.MediaTupleData.FileInfo;
import eu.europeana.cloud.dps.topologies.media.support.StatsTupleData;

public class DownloadBolt extends HttpClientBolt {
	
	private static final Logger logger = LoggerFactory.getLogger(DownloadBolt.class);
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(MediaTupleData.FIELD_NAME, StatsTupleData.FIELD_NAME));
		declarer.declareStream(StatsTupleData.STREAM_ID, new Fields(StatsTupleData.FIELD_NAME));
	}
	
	@Override
	protected HttpAsyncRequestProducer createRequestProducer(FileInfo fileInfo) {
		HttpGet request = new HttpGet(fileInfo.getUrl());
		request.setConfig(RequestConfig.custom()
				.setMaxRedirects(2)
				.setConnectTimeout(2000)
				.setSocketTimeout(5000)
				.build());
		return HttpAsyncMethods.create(request);
	}
	
	@Override
	protected ZeroCopyConsumer<Void> createResponseConsumer(FileInfo fileInfo, StatsTupleData stats, Tuple tuple)
			throws IOException {
		java.io.File content = java.io.File.createTempFile("media", null);
		return new ZeroCopyConsumer<Void>(content) {
			
			@Override
			protected Void process(HttpResponse response, java.io.File file, ContentType contentType) throws Exception {
				int status = response.getStatusLine().getStatusCode();
				long byteCount = file.length();
				if (status < 200 || status >= 300 || byteCount == 0) {
					logger.info("Download error (code {}) for {}", status, fileInfo.getUrl());
					if (file.exists() && !file.delete())
						logger.warn("Could not remove temp file {}", file);
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
				statusUpdate(OK);
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
					if (content.exists() && !content.delete())
						logger.warn("Could not remove temp file {}", content);
					statusUpdate("CONNECTION ERROR: " + msg);
				}
			}
			
			private void statusUpdate(String status) {
				synchronized (stats) {
					stats.addError(status);
					if (stats.getErrors().size() == stats.getResourceCount()) {
						boolean someSuccess = stats.getErrors().removeIf(OK::equals);
						if (someSuccess) {
							MediaTupleData data = (MediaTupleData) tuple.getValueByField(MediaTupleData.FIELD_NAME);
							data.getFileInfos().removeIf(f -> f.getContent() == null);
							outputCollector.emit(tuple, new Values(data, stats));
						} else {
							outputCollector.emit(StatsTupleData.STREAM_ID, tuple, new Values(stats));
						}
						outputCollector.ack(tuple);
					}
				}
			}
		};
	}
	
}
