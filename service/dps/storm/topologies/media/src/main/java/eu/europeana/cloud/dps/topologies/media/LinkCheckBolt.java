package eu.europeana.cloud.dps.topologies.media;

import java.io.IOException;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.ContentDecoder;
import org.apache.http.nio.IOControl;
import org.apache.http.nio.client.methods.HttpAsyncMethods;
import org.apache.http.nio.protocol.AbstractAsyncResponseConsumer;
import org.apache.http.nio.protocol.HttpAsyncRequestProducer;
import org.apache.http.nio.protocol.HttpAsyncResponseConsumer;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.europeana.cloud.dps.topologies.media.support.MediaTupleData.FileInfo;
import eu.europeana.cloud.dps.topologies.media.support.StatsTupleData;

public class LinkCheckBolt extends HttpClientBolt {
	
	private static final Logger logger = LoggerFactory.getLogger(LinkCheckBolt.class);
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(StatsTupleData.STREAM_ID, new Fields(StatsTupleData.FIELD_NAME));
	}
	
	@Override
	protected HttpAsyncRequestProducer createRequestProducer(FileInfo fileInfo) {
		HttpHead request = new HttpHead(fileInfo.getUrl());
		request.setConfig(RequestConfig.custom()
				.setMaxRedirects(FOLLOW_REDIRECTS)
				.setConnectTimeout(2000)
				.setSocketTimeout(5000)
				.build());
		return HttpAsyncMethods.create(request);
	}
	
	@Override
	protected HttpAsyncResponseConsumer<Void> createResponseConsumer(FileInfo fileInfo, StatsTupleData stats,
			Tuple tuple) throws IOException {
		return new AbstractAsyncResponseConsumer<Void>() {
			
			final Object lock = stats;
			
			@Override
			protected void onResponseReceived(HttpResponse response) throws HttpException, IOException {
				int status = response.getStatusLine().getStatusCode();
				if (status < 200 || status >= 300) {
					logger.info("Link error (code {}) for {}", status, fileInfo.getUrl());
					statusUpdate("STATUS CODE " + status);
					return;
				}
				Header[] contentTypeHeader = response.getHeaders(HTTP.CONTENT_TYPE);
				String contentType = contentTypeHeader.length == 1 ? contentTypeHeader[0].getValue() : "[NONE]";
				if (!contentType.startsWith("text/")) {
					logger.info("Invaild content type {} for {}", contentType, fileInfo.getUrl());
					statusUpdate("CONTENT TYPE " + contentType);
					return;
				}
				logger.debug("Link OK: {}", fileInfo.getUrl());
				statusUpdate(OK);
			}
			
			@Override
			protected void onContentReceived(ContentDecoder decoder, IOControl ioctrl) throws IOException {
				ioctrl.shutdown();
			}
			
			@Override
			protected void onEntityEnclosed(HttpEntity entity, ContentType contentType) throws IOException {
				// ignoring entity
			}
			
			@Override
			protected Void buildResult(HttpContext context) throws Exception {
				return null;
			}
			
			@Override
			protected void releaseResources() {
				Exception e = getException();
				if (e != null) {
					Object msg = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
					logger.info("Link exception ({}) for {}", msg, fileInfo.getUrl());
					logger.trace("Link failure details:", e);
					statusUpdate("CONNECTION ERROR: " + msg);
				}
			}
			
			private void statusUpdate(String status) {
				synchronized (lock) {
					stats.addError(status);
					if (stats.getErrors().size() == stats.getResourceCount()) {
						stats.getErrors().removeIf(OK::equals);
						outputCollector.emit(StatsTupleData.STREAM_ID, tuple, new Values(stats));
						outputCollector.ack(tuple);
					}
				}
			}
		};
	}
	
}
