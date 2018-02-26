package eu.europeana.cloud.dps.topologies.media;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.nio.client.methods.HttpAsyncMethods;
import org.apache.http.nio.client.methods.ZeroCopyConsumer;
import org.apache.http.pool.ConnPoolControl;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.europeana.cloud.dps.topologies.media.support.MediaTupleData;
import eu.europeana.cloud.dps.topologies.media.support.MediaTupleData.FileInfo;
import eu.europeana.cloud.dps.topologies.media.support.StatsTupleData;
import eu.europeana.cloud.dps.topologies.media.support.TempFileSync;

public class DownloadBolt extends BaseRichBolt {
	
	/** Temporary "error" value for counting successful downloads */
	private static final String OK = "OK";
	
	private static final Logger logger = LoggerFactory.getLogger(DownloadBolt.class);
	
	private OutputCollector outputCollector;
	
	private CloseableHttpAsyncClient httpClient;
	private ConnPoolControl<HttpRoute> connPoolControl;
	
	private InetAddress localAddress;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		
		outputCollector = collector;
		
		try {
			PoolingNHttpClientConnectionManager connMgr =
					new PoolingNHttpClientConnectionManager(new DefaultConnectingIOReactor());
			connMgr.setDefaultMaxPerRoute(
					(int) (long) stormConf.getOrDefault("MEDIATOPOLOGY_CONNECTIONS_PER_SOURCE", 4));
			connMgr.setMaxTotal((int) (long) stormConf.getOrDefault("MEDIATOPOLOGY_CONNECTIONS_TOTAL", 200));
			httpClient = HttpAsyncClients.custom().setConnectionManager(connMgr).build();
			httpClient.start();
			connPoolControl = connMgr;
		} catch (IOException e) {
			throw new RuntimeException("Could not initialize http client", e);
		}
		
		localAddress = TempFileSync.startServer(stormConf);
	}
	
	@Override
	public void cleanup() {
		try {
			httpClient.close();
		} catch (IOException e) {
			logger.error("HttpClient could not close", e);
		}
		TempFileSync.stopServer();
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(MediaTupleData.FIELD_NAME, StatsTupleData.FIELD_NAME));
		declarer.declareStream(StatsTupleData.STREAM_ID, new Fields(StatsTupleData.FIELD_NAME));
	}
	
	@Override
	public void execute(Tuple input) {
		MediaTupleData data = (MediaTupleData) input.getValueByField(MediaTupleData.FIELD_NAME);
		List<FileInfo> files = data.getFileInfos();
		StatsTupleData stats = new StatsTupleData(data.getTaskId(), files.size());
		
		List<ZeroCopyConsumer<Void>> fileStorers = new ArrayList<>();
		try {
			for (FileInfo fileInfo : files)
				fileStorers.add(createFileStorer(fileInfo, stats, input));
		} catch (IOException e) {
			logger.error("Disk error", e);
			outputCollector.fail(input);
			return;
		}
		for (int i = 0; i < files.size(); i++) {
			httpClient.execute(HttpAsyncMethods.createGet(files.get(i).getUrl()), fileStorers.get(i), null);
		}
	}
	
	private ZeroCopyConsumer<Void> createFileStorer(FileInfo fileInfo, StatsTupleData stats, Tuple tuple)
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
				
				long now = System.currentTimeMillis();
				stats.setDownloadEndTime(now);
				if (stats.getDownloadStartTime() == 0) {
					// too hard to determine actual start time, not significant for global measurement
					stats.setDownloadStartTime(now);
				}
				stats.setDownloadedBytes(stats.getDownloadedBytes() + byteCount);
				
				statusUpdate(OK);
				return null;
			}
			
			@Override
			protected void releaseResources() {
				super.releaseResources();
				Exception e = getException();
				if (e != null) {
					logger.info("Download exception ({}) for {}", e.getMessage(), fileInfo.getUrl());
					logger.trace("Download failure details:", e);
					if (content.exists() && !content.delete())
						logger.warn("Could not remove temp file {}", content);
					statusUpdate("CONNECTION ERROR");
				}
			}

			private void statusUpdate(String status) {
				stats.addError(status);
				if (stats.getErrors().size() == stats.getResourceCount()) {
					boolean someSuccess = stats.getErrors().removeIf(OK::equals);
					
					MediaTupleData data = (MediaTupleData) tuple.getValueByField(MediaTupleData.FIELD_NAME);
					data.getFileInfos().removeIf(f -> f.getContent() == null);
					
					logger.debug("Completed resource download for {} ({} files, {} bytes)",
							data.getEdmRepresentation().getCloudId(), data.getFileInfos().size(),
							stats.getDownloadedBytes());
					if (someSuccess) {
						outputCollector.emit(tuple, new Values(data, stats));
					} else {
						outputCollector.emit(StatsTupleData.STREAM_ID, tuple, new Values(stats));
					}
					outputCollector.ack(tuple);
				}
			}
		};
	}
	
}
