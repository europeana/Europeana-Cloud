package eu.europeana.cloud.service.dps.storm.topologies.text;

import backtype.storm.topology.OutputFieldsDeclarer;
import com.google.gson.Gson;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.transform.text.TextExtractor;
import eu.europeana.cloud.service.dps.storm.transform.text.TextExtractorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.Charset;
import java.util.Map;

/**
 * Bolt for text extracting.
 * It uses {@link DpsTask} parameter with key {@link PluginParameterKeys.EXTRACTOR} for determine which method should be used.
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class ExtractTextBolt extends AbstractDpsBolt {
    private final String storeStremName;
    private final String defaultStreamName;
    private static final Logger LOGGER = LoggerFactory.getLogger(ExtractTextBolt.class);

    /**
     * Constructor of extract text bolt with default stream.
     */
    public ExtractTextBolt() {
        this(null, null);
    }

    /**
     * Constructor of extract text bolt with one stream.
     *
     * @param defaultStreamName stream name - null => use default stream
     */
    public ExtractTextBolt(String defaultStreamName) {
        this(defaultStreamName, null);
    }

    /**
     * Constructor of extract text bolt with two streams.
     * If defaultStreamName and storeStremName are null then it use default stream.
     *
     * @param defaultStreamName name of stream for emit task if {@link PluginParameterKeys#STORE_EXTRACTED_TEXT} is false
     * @param storeStremName    name of stream for emit task if {@link PluginParameterKeys#STORE_EXTRACTED_TEXT} is true
     */
    public ExtractTextBolt(String defaultStreamName, String storeStremName) {
        if (storeStremName == null) {
            storeStremName = defaultStreamName;
        } else if (defaultStreamName == null) {
            defaultStreamName = storeStremName;
        }

        this.storeStremName = storeStremName;
        this.defaultStreamName = defaultStreamName;
    }

    @Override
    public void execute(StormTaskTuple t) {
        String representationName = t.getParameter(PluginParameterKeys.REPRESENTATION_NAME);
        String fileFormats = t.getParameter(PluginParameterKeys.FILE_FORMATS);
        String extractorName;
        if (fileFormats != null && !fileFormats.isEmpty()) {
            Map<String, String> formats = new Gson().fromJson(fileFormats, Map.class);
            String format = formats.get(representationName);
            if (format != null && !format.isEmpty()) {
                extractorName = format;
            } else {
                extractorName = representationName;
            }
        } else {
            extractorName = representationName;
        }

        String extractors = t.getParameter(PluginParameterKeys.EXTRACTORS);
        String extractionMetodName = null;
        if (extractors != null && !extractors.isEmpty()) {
            Map<String, String> extractors_ = new Gson().fromJson(extractors, Map.class);
            extractionMetodName = extractors_.get(extractorName);
        }
        TextExtractor extractor = TextExtractorFactory.getExtractor(extractorName, extractionMetodName);

        if (extractor == null) {
            String message = String.format("Extractor does not exist for extractor name %s.", extractorName);
            LOGGER.warn(message);
            emitDropNotification(t.getTaskId(), t.getFileUrl(), message, t.getParameters().toString());
            return;
        } else {
            LOGGER.info("Extractor name: {}, Required extraction method: {}, Selected extraction method: {}",
                    extractorName, extractionMetodName, extractor.getExtractionMethod().name());
        }

        ByteArrayInputStream data = t.getFileByteDataAsStream();
        String extractedText = extractor.extractText(data);
        Map<String, String> metadata = extractor.getExtractedMetadata();

        if (extractedText != null && !extractedText.isEmpty()) {
            t.setFileData(extractedText.getBytes(Charset.forName("UTF-8")));
            t.addParameter(PluginParameterKeys.MIME_TYPE, "text/plain");
            t.addParameter(PluginParameterKeys.REPRESENTATION_NAME, extractor.getRepresentationName());
            t.addParameter(PluginParameterKeys.ORIGINAL_FILE_URL, t.getFileUrl());

            if (metadata != null && !metadata.isEmpty()) {
                t.addParameter(PluginParameterKeys.FILE_METADATA, new Gson().toJson(metadata));
            }

            if (storeStremName != null && defaultStreamName != null) {
                //store extracted text?
                if (Boolean.parseBoolean(t.getParameter(PluginParameterKeys.STORE_EXTRACTED_TEXT))) {
                    outputCollector.emit(storeStremName, inputTuple, t.toStormTuple());
                } else {
                    outputCollector.emit(defaultStreamName, inputTuple, t.toStormTuple());
                }
            } else {
                outputCollector.emit(inputTuple, t.toStormTuple());
            }
        } else {
            String message = String.format("Cannot extract data from %s by %s. (or file is empty)",
                    extractorName, extractor.getExtractionMethod().name());
            emitDropNotification(t.getTaskId(), t.getFileUrl(), message, t.getParameters().toString());
        }

    }

    @Override
    public void prepare() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (storeStremName != null && defaultStreamName != null) {
            if (!storeStremName.equals(defaultStreamName)) {
                //store branch
                declarer.declareStream(storeStremName, StormTaskTuple.getFields());
            }

            //default branch
            declarer.declareStream(defaultStreamName, StormTaskTuple.getFields());
        } else {
            declarer.declare(StormTaskTuple.getFields());
        }

        //notifications
        declarer.declareStream(NOTIFICATION_STREAM_NAME, NotificationTuple.getFields());
    }
}
