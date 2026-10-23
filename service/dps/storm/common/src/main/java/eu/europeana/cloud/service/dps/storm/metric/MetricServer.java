package eu.europeana.cloud.service.dps.storm.metric;

import static eu.europeana.cloud.service.dps.storm.utils.TopologySubmitter.START_ON_LOCAL_CLUSTER;

import eu.europeana.cloud.service.dps.storm.utils.TopologySubmitter;
import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopologySubmitter.class);
    private static HTTPServer server;
    private static final AtomicBoolean started = new AtomicBoolean(false);

    public static synchronized void start(int workerPort) throws IOException {
        if (START_ON_LOCAL_CLUSTER) {
            LOGGER.warn("Metrics are turned off, because topology runs on the local cluster embedded into this java process. This is truned off to not conflict port with other local cluster topologies.");
            return;
        }

        if (started.compareAndSet(false, true)) {
            server = HTTPServer.builder()
                    .port(workerPort + 3000)
                    .buildAndStart();

            JvmMetrics.builder().register();
        }
    }

    public static synchronized void stop() throws IOException {
        if (server != null) {
            server.stop();
            server = null;
        }
    }
}
