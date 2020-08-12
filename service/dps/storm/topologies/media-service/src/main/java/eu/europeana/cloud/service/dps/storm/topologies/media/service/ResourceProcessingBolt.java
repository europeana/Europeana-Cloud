package eu.europeana.cloud.service.dps.storm.topologies.media.service;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.gson.Gson;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.metis.mediaprocessing.MediaExtractor;
import eu.europeana.metis.mediaprocessing.MediaProcessorFactory;
import eu.europeana.metis.mediaprocessing.exception.MediaProcessorException;
import eu.europeana.metis.mediaprocessing.model.RdfResourceEntry;
import eu.europeana.metis.mediaprocessing.model.ResourceExtractionResult;
import eu.europeana.metis.mediaprocessing.model.Thumbnail;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * Created by Tarek on 12/11/2018.
 */
public class ResourceProcessingBolt extends AbstractDpsBolt {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(ResourceProcessingBolt.class);
    private static final String MEDIA_RESOURCE_EXCEPTION = "media resource exception";

    private AmazonClient amazonClient;

    private transient Gson gson;
    private transient MediaExtractor mediaExtractor;

    public ResourceProcessingBolt(AmazonClient amazonClient) {
        this.amazonClient = amazonClient;
    }


    @Override
    public void execute(StormTaskTuple stormTaskTuple) {
        LOGGER.info("Starting resource processing");
        long processingStartTime = new Date().getTime();
        StringBuilder exception = new StringBuilder();
        if (stormTaskTuple.getParameter(PluginParameterKeys.RESOURCE_LINKS_COUNT) == null) {
            outputCollector.emit(stormTaskTuple.toStormTuple());
        } else {
            try {
                RdfResourceEntry rdfResourceEntry = gson.fromJson(stormTaskTuple.getParameter(PluginParameterKeys.RESOURCE_LINK_KEY), RdfResourceEntry.class);
                ResourceExtractionResult resourceExtractionResult = mediaExtractor.performMediaExtraction(rdfResourceEntry, Boolean.parseBoolean(stormTaskTuple.getParameter(PluginParameterKeys.MAIN_THUMBNAIL_AVAILABLE)));
                if (resourceExtractionResult != null) {
                    if (resourceExtractionResult.getMetadata() != null)
                        stormTaskTuple.addParameter(PluginParameterKeys.RESOURCE_METADATA, gson.toJson(resourceExtractionResult.getMetadata()));
                    storeThumbnails(stormTaskTuple, exception, resourceExtractionResult);
                }
            } catch (Exception e) {
                LOGGER.error("Exception while processing the resource {}. The full error is:{} ", stormTaskTuple.getParameter(PluginParameterKeys.RESOURCE_URL), ExceptionUtils.getStackTrace(e));
                buildErrorMessage(exception, "Exception while processing the resource: " + stormTaskTuple.getParameter(PluginParameterKeys.RESOURCE_URL) + ". The full error is: " + e.getMessage() + " because of: " + e.getCause());
            } finally {
                stormTaskTuple.getParameters().remove(PluginParameterKeys.RESOURCE_LINK_KEY);
                if (exception.length() > 0) {
                    stormTaskTuple.addParameter(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE, exception.toString());
                    stormTaskTuple.addParameter(PluginParameterKeys.UNIFIED_ERROR_MESSAGE, MEDIA_RESOURCE_EXCEPTION);
                }
                outputCollector.emit(stormTaskTuple.toStormTuple());

            }
        }
        LOGGER.info("Resource processing finished in: {}ms", Calendar.getInstance().getTimeInMillis() - processingStartTime);
    }

    private void storeThumbnails(StormTaskTuple stormTaskTuple, StringBuilder exception, ResourceExtractionResult resourceExtractionResult) throws IOException {
        List<Thumbnail> thumbnails = resourceExtractionResult.getThumbnails();
        if (thumbnails != null) {
            for (Thumbnail thumbnail : thumbnails) {
                if (taskStatusChecker.hasKillFlag(stormTaskTuple.getTaskId()))
                    break;
                try (InputStream stream = thumbnail.getContentStream()) {
                    amazonClient.putObject(thumbnail.getTargetName(), stream, prepareObjectMetadata(thumbnail));
                } catch (Exception e) {
                    String errorMessage = "Error while uploading " + thumbnail.getTargetName() + " to S3 in Bluemix. The full error message is: " + e.getMessage() + " because of: " + e.getCause();
                    LOGGER.error(errorMessage, e);
                    buildErrorMessage(exception, errorMessage);

                } finally {
                    thumbnail.close();
                }
            }
        }
    }

    @Override
    public void prepare() {
        try {
            createMediaExtractor();
            initGson();
            amazonClient.init();
        } catch (Exception e) {
            LOGGER.error("Error while initialization", e);
            throw new RuntimeException(e);
        }
    }

    void initGson() {
        gson = new Gson();
    }

    private void createMediaExtractor() throws MediaProcessorException {
        mediaExtractor = new MediaProcessorFactory().createMediaExtractor();
    }

    private ObjectMetadata prepareObjectMetadata(Thumbnail thumbnail) throws IOException {
        final ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentType(thumbnail.getMimeType());
        objectMetadata.setContentLength(thumbnail.getContentSize());
        return objectMetadata;
    }

    private void buildErrorMessage(StringBuilder message, String newMessage) {
        LOGGER.error("Error while processing {}", newMessage);
        if (message.toString().isEmpty()) {
            message.append(newMessage);
        } else {
            message.append(", ").append(newMessage);
        }
    }
}
