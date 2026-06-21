package eu.europeana.cloud.service.dps.storm.metric;

import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;

import java.io.IOException;

public class MetricServer {

    private static HTTPServer server;


    public static synchronized void start(int workerPort) throws IOException {
        if (server == null) {
            server = HTTPServer.builder()
                    .port(workerPort + 3000)
                    .buildAndStart();
            JvmMetrics.builder().register();
        }
    }

    public static synchronized void stop() throws IOException {
        if (server != null) {
            server.stop();
        }
    }
}
