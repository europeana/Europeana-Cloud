package eu.europeana.cloud.service.dps.storm.metric;

import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Histogram;

public final class MetricRegistry {

    private static volatile boolean initialized = false;

    private static Counter PROCESSED;
    private static Counter MESSAGE_PROCESSED;
    private static Counter FAILED;
    private static Counter MESSAGE_FAILED;

    private static Histogram PROCESSING_LATENCY;
    private static Histogram WR_UPLOAD_FILE_LATENCY;
    private static Histogram RF_READ_FILE_LATENCY;
    private static Histogram RW_ADD_REVISION_LATENCY;
    private static Histogram PF_GET_RESOURCES_FROM_RDF_LATENCY;
    private static Histogram OAI_FILE_DOWNLOAD_LATENCY;
    private static Histogram HTTP_FILE_DOWNLOAD_LATENCY;
    private static Histogram XML_FILE_SIZE;
    private static Histogram SPOUT_EMIT_TO_ACK_LATENCY;
    private static Histogram XSLT_TRANSFORM_LATENCY;
    private static Histogram DUPLICATE_DETECTION_LATENCY;
    private static Histogram VALIDATION_LATENCY;
    private static Histogram NORMALIZATION_LATENCY;
    private static Histogram LINK_CHECK_LATENCY;
    private static Histogram INDEXING_EUROPEANA_ID_FINDER_LATENCY;
    private static Histogram INDEXING_LATENCY;
    private static Histogram INDEXING_REMOVE_TOMBSTONE_LATENCY;
    private static Histogram INDEXING_REMOVE_INDEX_RECORD_LATENCY;
    private static Histogram ENRICHMENT_LATENCY;
    private static Histogram DEPUBLISH_LATENCY;
    private static Histogram MEDIA_EDM_ENRICHMENT_LATENCY;
    private static Histogram MEDIA_EDM_METADATA_ENRICHMENT_LATENCY;
    private static Histogram MEDIA_OP_MAIN_THUMBNAIL_EXTRACTION_LATENCY;
    private static Histogram MEDIA_OP_REMAINING_RESOURCE_EXTRACTION_LATENCY;
    private static Histogram MEDIA_OP_THUMBNAIL_STORING_LATENCY;
    private static Histogram MEDIA_RP_MEDIA_EXTRACTION_LATENCY;
    private static Histogram MEDIA_RP_THUMBNAIL_STORING_LATENCY;

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
        MESSAGE_PROCESSED = Counter.builder()
                .name("storm_message_processed_total")
                .help("Total number of messages processed by storm")
                .labelNames("topology", "component")
                .register();
        MESSAGE_FAILED = Counter.builder()
                .name("storm_message_failed_total")
                .help("Total number of messages failed by storm")
                .labelNames("topology", "component")
                .register();
        PROCESSING_LATENCY = Histogram.builder()
                .name("storm_processing_latency")
                .help("Storm processing latency")
                .classicExponentialUpperBounds(
                        0.005, 2, 15
                )
                .labelNames("topology", "component")
                .register();
        WR_UPLOAD_FILE_LATENCY = Histogram.builder()
                .name("write_record_upload_file_latency")
                .help("Write record - upload file latency")
                .classicExponentialUpperBounds(
                        0.005, 2, 15
                )
                .labelNames("topology", "component")
                .register();
        RF_READ_FILE_LATENCY = Histogram.builder()
                .name("read_file_latency")
                .help("Read file - read file latency")
                .classicExponentialUpperBounds(
                        0.005, 2, 15
                )
                .labelNames("topology", "component")
                .register();
        RW_ADD_REVISION_LATENCY = Histogram.builder()
                .name("revision_writer_add_revision_latency")
                .help("Revision writer - add revision latency")
                .classicExponentialUpperBounds(
                        0.005, 2, 15
                )
                .labelNames("topology", "component")
                .register();
        PF_GET_RESOURCES_FROM_RDF_LATENCY = Histogram.builder()
                .name("parse_file_get_resources_from_rdf_latency")
                .help("Parse file - get resources from RDF latency")
                .classicExponentialUpperBounds(
                        0.005, 2, 15
                )
                .labelNames("topology", "component")
                .register();
        OAI_FILE_DOWNLOAD_LATENCY = Histogram.builder()
                .name("oai_file_download_latency")
                .help("OAI file download latency")
                .classicExponentialUpperBounds(
                        0.005, 2, 15
                )
                .labelNames("topology", "component")
                .register();
        HTTP_FILE_DOWNLOAD_LATENCY = Histogram.builder()
                .name("http_file_download_latency")
                .help("HTTP file download latency")
                .classicExponentialUpperBounds(
                        0.005, 2, 15
                )
                .labelNames("topology", "component")
                .register();
        XML_FILE_SIZE = Histogram.builder()
                .name("xml_file_size")
                .help("XML file size")
                .classicExponentialUpperBounds(
                        0.005, 2, 15
                )
                .labelNames("topology", "component")
                .register();
        XSLT_TRANSFORM_LATENCY = Histogram.builder()
                .name("xslt_transform_latency")
                .help("XSLT transform latency")
                .classicExponentialUpperBounds(
                        0.005, 2, 15
                )
                .labelNames("topology", "component")
                .register();
        VALIDATION_LATENCY = Histogram.builder()
                .name("validation_latency")
                .help("Validation latency")
                .classicExponentialUpperBounds(
                        0.005, 2, 15
                )
                .labelNames("topology", "component")
                .register();
        DUPLICATE_DETECTION_LATENCY = Histogram.builder()
                .name("duplicate_detection_latency")
                .help("Duplicate detection latency")
                .classicExponentialUpperBounds(
                        0.005, 2, 15
                )
                .labelNames("topology", "component")
                .register();
        NORMALIZATION_LATENCY = Histogram.builder()
                .name("normalization_latency")
                .help("Normalization latency")
                .classicExponentialUpperBounds(
                        0.005, 2, 15
                )
                .labelNames("topology", "component")
                .register();
        LINK_CHECK_LATENCY = Histogram.builder()
                .name("link_check_latency")
                .help("Link check latency")
                .classicExponentialUpperBounds(
                        0.005, 2, 15
                )
                .labelNames("topology", "component")
                .register();
        INDEXING_EUROPEANA_ID_FINDER_LATENCY = Histogram.builder()
                .name("indexing_europeana_id_finder_latency")
                .help("Indexing Europeana ID finder latency")
                .classicExponentialUpperBounds(
                        0.005, 2, 15
                )
                .labelNames("topology", "component")
                .register();
        INDEXING_REMOVE_TOMBSTONE_LATENCY = Histogram.builder()
                .name("indexing_remove_tombstone_latency")
                .help("Indexing remove tombstone latency")
                .classicExponentialUpperBounds(
                        0.005, 2, 15
                )
                .labelNames("topology", "component")
                .register();
        INDEXING_REMOVE_INDEX_RECORD_LATENCY = Histogram.builder()
                .name("indexing_remove_index_record_latency")
                .help("Indexing remove index record latency")
                .classicExponentialUpperBounds(
                        0.005, 2, 15
                )
                .labelNames("topology", "component")
                .register();
        INDEXING_LATENCY = Histogram.builder()
                .name("indexing_latency")
                .help("Indexing latency")
                .classicExponentialUpperBounds(
                        0.005, 2, 15
                )
                .labelNames("topology", "component")
                .register();

        ENRICHMENT_LATENCY = Histogram.builder()
                .name("enrichment_latency")
                .help("Enrichment latency")
                .classicExponentialUpperBounds(
                        0.005, 2, 15
                )
                .labelNames("topology", "component")
                .register();
        SPOUT_EMIT_TO_ACK_LATENCY = Histogram.builder()
                .name("spout_emit_to_ack_latency")
                .help("End-to-end processing latency (spout emit → ack)")
                .classicExponentialUpperBounds(
                        0.005, 2, 15
                )
                .labelNames("topology", "component")
                .register();
        DEPUBLISH_LATENCY = Histogram.builder()
                .name("depublish_latency")
                .help("Depublish latency")
                .classicExponentialUpperBounds(
                        0.005, 2, 15
                )
                .labelNames("topology", "component")
                .register();
        MEDIA_EDM_ENRICHMENT_LATENCY = Histogram.builder()
                .name("media_edm_enrichment_latency")
                .help("Media EDM enrichment latency")
                .classicExponentialUpperBounds(
                        0.005, 2, 15
                )
                .labelNames("topology", "component")
                .register();
        MEDIA_EDM_METADATA_ENRICHMENT_LATENCY = Histogram.builder()
                .name("media_edm_metadata_enrichment_latency")
                .help("Media EDM metadata enrichment latency")
                .classicExponentialUpperBounds(
                        0.005, 2, 15
                )
                .labelNames("topology", "component")
                .register();
        MEDIA_OP_MAIN_THUMBNAIL_EXTRACTION_LATENCY = Histogram.builder()
                .name("media_OP_main_thumbnail_extraction_latency")
                .help("Media object processor main thumbnail extraction latency")
                .classicExponentialUpperBounds(
                        0.005, 2, 15
                )
                .labelNames("topology", "component")
                .register();
        MEDIA_OP_REMAINING_RESOURCE_EXTRACTION_LATENCY = Histogram.builder()
                .name("media_OP_remaining_resource_extraction_latency")
                .help("Media remaining resource extraction latency")
                .classicExponentialUpperBounds(
                        0.005, 2, 15
                )
                .labelNames("topology", "component")
                .register();

        MEDIA_OP_THUMBNAIL_STORING_LATENCY = Histogram.builder()
                .name("media_OP_thumbnail_storing_latency")
                .help("Media thumbnail storing latency")
                .classicExponentialUpperBounds(
                        0.005, 2, 15
                )
                .labelNames("topology", "component")
                .register();

        MEDIA_RP_MEDIA_EXTRACTION_LATENCY = Histogram.builder()
                .name("media_RP_media_extraction_latency")
                .help("Media resource processing media extraction latency")
                .classicExponentialUpperBounds(
                        0.005, 2, 15
                )
                .labelNames("topology", "component")
                .register();

        MEDIA_RP_THUMBNAIL_STORING_LATENCY = Histogram.builder()
                .name("media_RP_thumbnail_storing_latency")
                .help("Media resource processing thumbnail storing latency")
                .classicExponentialUpperBounds(
                        0.005, 2, 15
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

    public static void messageProcessed(String topology, String component) {
        ensureInit();
        MESSAGE_PROCESSED.labelValues(
                safe(topology),
                safe(component)
        ).inc();
    }

    public static void messageFailed(String topology, String component) {
        ensureInit();
        MESSAGE_FAILED.labelValues(
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

    public static void spoutEmitToAckLatency(String topology, String component, double seconds) {
        ensureInit();
        SPOUT_EMIT_TO_ACK_LATENCY.labelValues(
                safe(topology),
                safe(component)
        ).observe(seconds);
    }

    public static void wrBoltUploadFileLatency(String topology, String component, double seconds) {
        ensureInit();
        WR_UPLOAD_FILE_LATENCY.labelValues(
                safe(topology),
                safe(component)
        ).observe(seconds);
    }

    public static void rfBoltReadFileLatency(String topology, String component, double seconds) {
        ensureInit();
        RF_READ_FILE_LATENCY.labelValues(
                safe(topology),
                safe(component)
        ).observe(seconds);
    }

    public static void rwAddRevisionLatency(String topology, String component, double seconds) {
        ensureInit();
        RW_ADD_REVISION_LATENCY.labelValues(
                safe(topology),
                safe(component)
        ).observe(seconds);
    }

    public static void pfGetResourcesFromRdfLatency(String topology, String component, double seconds) {
        ensureInit();
        PF_GET_RESOURCES_FROM_RDF_LATENCY.labelValues(
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

    public static void xsltTransformLatency(String topology, String component, double seconds) {
        ensureInit();
        XSLT_TRANSFORM_LATENCY.labelValues(
                safe(topology),
                safe(component)
        ).observe(seconds);
    }

    public static void duplicateDetectionLatency(String topology, String component, double seconds) {
        ensureInit();
        DUPLICATE_DETECTION_LATENCY.labelValues(
                safe(topology),
                safe(component)
        ).observe(seconds);
    }

    public static void validationLatency(String topology, String component, double seconds) {
        ensureInit();
        VALIDATION_LATENCY.labelValues(
                safe(topology),
                safe(component)
        ).observe(seconds);
    }

    public static void normalizationLatency(String topology, String component, double seconds) {
        ensureInit();
        NORMALIZATION_LATENCY.labelValues(
                safe(topology),
                safe(component)
        ).observe(seconds);
    }

    public static void linkCheckLatency(String topology, String component, double seconds) {
        ensureInit();
        LINK_CHECK_LATENCY.labelValues(
                safe(topology),
                safe(component)
        ).observe(seconds);
    }

    public static void indexingEuropeanaIdFinderLatency(String topology, String component, double seconds) {
        ensureInit();
        INDEXING_EUROPEANA_ID_FINDER_LATENCY.labelValues(
                safe(topology),
                safe(component)
        ).observe(seconds);
    }

    public static void indexingLatency(String topology, String component, double seconds) {
        ensureInit();
        INDEXING_LATENCY.labelValues(
                safe(topology),
                safe(component)
        ).observe(seconds);
    }

    public static void indexingRemoveTombstoneLatency(String topology, String component, double seconds) {
        ensureInit();
        INDEXING_REMOVE_TOMBSTONE_LATENCY.labelValues(
                safe(topology),
                safe(component)
        ).observe(seconds);
    }

    public static void indexingRemoveIndexRecordLatency(String topology, String component, double seconds) {
        ensureInit();
        INDEXING_REMOVE_INDEX_RECORD_LATENCY.labelValues(
                safe(topology),
                safe(component)
        ).observe(seconds);
    }

    public static void enrichmentLatency(String topology, String component, double seconds) {
        ensureInit();
        ENRICHMENT_LATENCY.labelValues(
                safe(topology),
                safe(component)
        ).observe(seconds);
    }

    public static void depublishLatency(String topology, String component, double seconds) {
        ensureInit();
        DEPUBLISH_LATENCY.labelValues(
                safe(topology),
                safe(component)
        ).observe(seconds);
    }

    public static void mediaEdmEnrichmentLatency(String topology, String component, double seconds) {
        ensureInit();
        MEDIA_EDM_ENRICHMENT_LATENCY.labelValues(
                safe(topology),
                safe(component)
        ).observe(seconds);
    }

    public static void mediaEdmMetadataEnrichmentLatency(String topology, String component, double seconds) {
        ensureInit();
        MEDIA_EDM_METADATA_ENRICHMENT_LATENCY.labelValues(
                safe(topology),
                safe(component)
        ).observe(seconds);
    }

    public static void mediaOpMainThumbnailExtractionLatency(String topology, String component, double seconds) {
        ensureInit();
        MEDIA_OP_MAIN_THUMBNAIL_EXTRACTION_LATENCY.labelValues(
                safe(topology),
                safe(component)
        ).observe(seconds);
    }

    public static void mediaOpRemainingResourceExtractionLatency(String topology, String component, double seconds) {
        ensureInit();
        MEDIA_OP_REMAINING_RESOURCE_EXTRACTION_LATENCY.labelValues(
                safe(topology),
                safe(component)
        ).observe(seconds);
    }

    public static void mediaOpThumbnailStoringLatency(String topology, String component, double seconds) {
        ensureInit();
        MEDIA_OP_THUMBNAIL_STORING_LATENCY.labelValues(
                safe(topology),
                safe(component)
        ).observe(seconds);
    }

    public static void mediaRpMediaExtractionLatency(String topology, String component, double seconds) {
        ensureInit();
        MEDIA_RP_MEDIA_EXTRACTION_LATENCY.labelValues(
                safe(topology),
                safe(component)
        ).observe(seconds);
    }

    public static void mediaRpThumbnailStoringLatency(String topology, String component, double seconds) {
        ensureInit();
        MEDIA_RP_THUMBNAIL_STORING_LATENCY.labelValues(
                safe(topology),
                safe(component)
        ).observe(seconds);
    }
}
