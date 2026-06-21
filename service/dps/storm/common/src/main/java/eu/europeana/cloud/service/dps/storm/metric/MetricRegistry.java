package eu.europeana.cloud.service.dps.storm.metric;

import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Histogram;

public final class MetricRegistry {

    public static Counter PROCESSED;

    public static Counter FAILED;

    public static Histogram PROCESSING_LATENCY;

    private MetricRegistry() {
    }


    private static volatile boolean initialized = false;

    public static synchronized void init() {
        if (initialized) return;
        PROCESSED = Counter.builder()
                .name("storm_processed_total")
                .help("Total number of records processed by storm")
                .labelNames("topology", "component", "task")
                .register();
        FAILED = Counter.builder()
                .name("storm_failed_total")
                .help("Total number of records failed by storm")
                .labelNames("topology", "component", "task")
                .register();
        PROCESSING_LATENCY = Histogram.builder()
                .name("storm_processing_latency")
                .help("Storm processing latency")
                .labelNames("topology", "component", "task")
                .register();
        initialized = true;
    }

}