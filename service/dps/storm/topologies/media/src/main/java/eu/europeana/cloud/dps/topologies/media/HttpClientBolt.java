package eu.europeana.cloud.dps.topologies.media;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpHost;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.nio.protocol.HttpAsyncRequestProducer;
import org.apache.http.nio.protocol.HttpAsyncResponseConsumer;
import org.apache.http.pool.ConnPoolControl;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.europeana.cloud.dps.topologies.media.support.MediaTupleData;
import eu.europeana.cloud.dps.topologies.media.support.MediaTupleData.FileInfo;
import eu.europeana.cloud.dps.topologies.media.support.StatsTupleData;
import eu.europeana.cloud.dps.topologies.media.support.TempFileSync;

abstract class HttpClientBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(HttpClientBolt.class);

    protected static final int FOLLOW_REDIRECTS = 3;

    protected transient OutputCollector outputCollector;

    protected transient CloseableHttpAsyncClient httpClient;
    private transient ConnPoolControl<HttpRoute> connPoolControl;
    private HashMap<String, Integer> connectionLimitsPerHost = new HashMap<>();
    private HashMap<HttpRoute, Integer> currentClientLimits = new HashMap<>();

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
    public void execute(Tuple input) {
        MediaTupleData data = (MediaTupleData) input.getValueByField(MediaTupleData.FIELD_NAME);
        updateConnectionLimits(data);
        List<FileInfo> files = data.getFileInfos();
        StatsTupleData stats = new StatsTupleData(data.getTaskId(), files.size());

        List<HttpAsyncResponseConsumer<Void>> responseConsumers = new ArrayList<>();
        try {
            for (FileInfo fileInfo : files)
                responseConsumers.add(createResponseConsumer(fileInfo, stats, input));
        } catch (IOException e) {
            logger.error("Disk error?", e);
            for (HttpAsyncResponseConsumer<Void> c : responseConsumers)
                c.failed(e);
            outputCollector.fail(input);
            return;
        }
        for (int i = 0; i < files.size(); i++) {
            httpClient.execute(createRequestProducer(files.get(i)), responseConsumers.get(i), null);
        }
    }

    private void updateConnectionLimits(MediaTupleData data) {
        connectionLimitsPerHost.putAll(data.getConnectionLimitsPerSource());
        for (FileInfo fileInfo : data.getFileInfos()) {
            URI uri = URI.create(fileInfo.getUrl());
            Integer limit = connectionLimitsPerHost.get(uri.getHost());
            if (limit == null)
                continue;
            boolean secure = "https".equals(uri.getScheme());
            HttpRoute route = new HttpRoute(new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme()), null, secure);
            Integer currentLimit = currentClientLimits.get(route);
            if (!limit.equals(currentLimit)) {
                connPoolControl.setMaxPerRoute(route, limit);
                currentClientLimits.put(route, limit);
            }
        }
    }

    protected abstract HttpAsyncRequestProducer createRequestProducer(FileInfo fileInfo);

    protected abstract HttpAsyncResponseConsumer<Void> createResponseConsumer(FileInfo fileInfo, StatsTupleData stats,
            Tuple input) throws IOException;
}
