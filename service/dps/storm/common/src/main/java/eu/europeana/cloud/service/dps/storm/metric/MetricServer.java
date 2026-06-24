package eu.europeana.cloud.service.dps.storm.metric;

import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class MetricServer {

    private static HTTPServer server;
    private static final AtomicBoolean started = new AtomicBoolean(false);

    public static synchronized void start(int workerPort) throws IOException {
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
