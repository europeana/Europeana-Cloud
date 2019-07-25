package eu.europeana.cloud.dps.topologies.media;

import java.io.IOException;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.ContentDecoder;
import org.apache.http.nio.IOControl;
import org.apache.http.nio.client.methods.HttpAsyncMethods;
import org.apache.http.nio.protocol.AbstractAsyncResponseConsumer;
import org.apache.http.nio.protocol.HttpAsyncRequestProducer;
import org.apache.http.nio.protocol.HttpAsyncResponseConsumer;
import org.apache.http.protocol.HttpContext;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.europeana.cloud.dps.topologies.media.support.MediaTupleData.FileInfo;
import eu.europeana.cloud.dps.topologies.media.support.StatsTupleData;
import eu.europeana.cloud.dps.topologies.media.support.StatsTupleData.Status;

public class LinkCheckBolt extends HttpClientBolt {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(LinkCheckBolt.class);

    private transient RequestConfig requestConfig;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        requestConfig = RequestConfig.custom()
                .setMaxRedirects(FOLLOW_REDIRECTS)
                .setConnectTimeout(2000)
                .setSocketTimeout(5000)
                .build();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(StatsTupleData.STREAM_ID, new Fields(StatsTupleData.FIELD_NAME));
    }

    @Override
    protected HttpAsyncRequestProducer createRequestProducer(FileInfo fileInfo) {
        HttpHead request = new HttpHead(fileInfo.getUrl());
        request.setConfig(requestConfig);
        return HttpAsyncMethods.create(request);
    }

    @Override
    protected HttpAsyncResponseConsumer<Void> createResponseConsumer(FileInfo fileInfo, StatsTupleData stats,
            Tuple tuple) throws IOException {
        return new HeadResponseConsumer(stats, fileInfo, tuple);
    }

    private abstract class ResponseConsumer extends AbstractAsyncResponseConsumer<Void> {
        final StatsTupleData stats;
        final FileInfo fileInfo;
        final Tuple tuple;

        protected ResponseConsumer(StatsTupleData stats, FileInfo fileInfo, Tuple tuple) {
            this.stats = stats;
            this.fileInfo = fileInfo;
            this.tuple = tuple;
        }

        @Override
        protected void onResponseReceived(HttpResponse response) throws HttpException, IOException {
            int status = response.getStatusLine().getStatusCode();
            if (status < 200 || status >= 300) {
                logger.info("Link error (code {}) for {}", status, fileInfo.getUrl());
                statusUpdate("STATUS CODE " + status);
                return;
            }
            logger.debug("Link OK: {}", fileInfo.getUrl());
            statusUpdate(Status.STATUS_OK);
        }

        @Override
        protected void onContentReceived(ContentDecoder decoder, IOControl ioctrl) throws IOException {
            logger.error("unexpected content received");
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
            synchronized (stats) {
                stats.addStatus(fileInfo.getUrl(), status);
                if (stats.getStatuses().size() == stats.getResourceCount()) {
                    outputCollector.emit(StatsTupleData.STREAM_ID, tuple, new Values(stats));
                    outputCollector.ack(tuple);
                }
            }
        }
    }

    private class HeadResponseConsumer extends ResponseConsumer {
        protected HeadResponseConsumer(StatsTupleData stats, FileInfo fileInfo, Tuple tuple) {
            super(stats, fileInfo, tuple);
        }

        @Override
        @SuppressWarnings("resource")
        protected void onResponseReceived(HttpResponse response) throws HttpException, IOException {
            int status = response.getStatusLine().getStatusCode();
            if (status >= 400 && status < 500) {
                logger.info("HEAD rejected ({}), retrying with GET: {}", response.getStatusLine(), fileInfo.getUrl());
                HttpGet request = new HttpGet(fileInfo.getUrl());
                request.setConfig(requestConfig);
                httpClient.execute(HttpAsyncMethods.create(request),
                        new GetResponseConsumer(stats, fileInfo, tuple),
                        null);
                return;
            }
            super.onResponseReceived(response);
        }
    }

    private class GetResponseConsumer extends ResponseConsumer {
        protected GetResponseConsumer(StatsTupleData stats, FileInfo fileInfo, Tuple tuple) {
            super(stats, fileInfo, tuple);
        }

        @Override
        protected void onResponseReceived(HttpResponse response) throws HttpException, IOException {
            cancel();
            super.onResponseReceived(response);
        }
    }
}
