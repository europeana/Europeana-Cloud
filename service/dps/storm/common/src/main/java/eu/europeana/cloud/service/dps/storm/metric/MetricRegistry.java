package eu.europeana.cloud.service.dps.storm.metric;

import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Histogram;

public final class MetricRegistry {

    private static volatile boolean initialized = false;

    private static Counter PROCESSED;
    private static Counter FAILED;

    private static Histogram PROCESSING_LATENCY;
    private static Histogram OAI_FILE_DOWNLOAD_LATENCY;
    private static Histogram HTTP_FILE_DOWNLOAD_LATENCY;
    private static Histogram XML_FILE_SIZE;
    private static Histogram SPOUT_EMIT_TO_ACK_LATENCY;

    private MetricRegistry() {
    }

    private static synchronized void init() {
        if (initialized) return;

        PROCESSED = Counter.builder()
                .name("storm_processed_total")
                .help("Total number of records processed by storm")
                .labelNames("topology", "component")
                .register();
        FAILED = Counter.builder()
                .name("storm_failed_total")
                .help("Total number of records failed by storm")
                .labelNames("topology", "component")
                .register();
        PROCESSING_LATENCY = Histogram.builder()
                .name("storm_processing_latency")
                .help("Storm processing latency")
                .labelNames("topology", "component")
                .register();
        OAI_FILE_DOWNLOAD_LATENCY = Histogram.builder()
                .name("oai_file_download_latency")
                .help("OAI file download latency")
                .labelNames("topology", "component")
                .register();
        HTTP_FILE_DOWNLOAD_LATENCY = Histogram.builder()
                .name("http_file_download_latency")
                .help("HTTP file download latency")
                .labelNames("topology", "component")
                .register();
        XML_FILE_SIZE = Histogram.builder()
                .name("xml_file_size")
                .help("XML file size")
                .classicExponentialUpperBounds(
                        0.20, 2, 9
                )
                .labelNames("topology", "component")
                .register();
        SPOUT_EMIT_TO_ACK_LATENCY = Histogram.builder()
                .name("spout_emit_to_ack_latency")
                .help("End-to-end processing latency (spout emit → ack)")
                .classicExponentialUpperBounds(
                        0.025, 2, 12
                )
                .labelNames("topology", "component")
                .register();
        initialized = true;
    }

    private static void ensureInit() {
        if (!initialized) {
            init();
        }
    }

    private static String safe(String value) {
        return (value == null || value.isBlank()) ? "unknown" : value;
    }

    public static void processed(String topology, String component) {
        ensureInit();
        PROCESSED.labelValues(
                safe(topology),
                safe(component)
        ).inc();
    }

    public static void failed(String topology, String component) {
        ensureInit();
        FAILED.labelValues(
                safe(topology),
                safe(component)
        ).inc();
    }


    public static void processingLatency(String topology, String component, double seconds) {
        ensureInit();
        PROCESSING_LATENCY.labelValues(
                safe(topology),
                safe(component)
        ).observe(seconds);
    }

    public static void oaiFileDownloadLatency(String topology, String component, double seconds) {
        ensureInit();
        OAI_FILE_DOWNLOAD_LATENCY.labelValues(
                safe(topology),
                safe(component)
        ).observe(seconds);
    }

    public static void httpFileDownloadLatency(String topology, String component, double seconds) {
        ensureInit();
        HTTP_FILE_DOWNLOAD_LATENCY.labelValues(
                safe(topology),
                safe(component)
        ).observe(seconds);
    }

    public static void xmlFileSize(String topology, String component, double size) {
        ensureInit();
        XML_FILE_SIZE.labelValues(
                safe(topology),
                safe(component)
        ).observe(size);
    }

    public static void spoutEmitToAckLatency(String topology, String component, double seconds) {
        ensureInit();
        SPOUT_EMIT_TO_ACK_LATENCY.labelValues(
                safe(topology),
                safe(component)
        ).observe(seconds);
    }
}