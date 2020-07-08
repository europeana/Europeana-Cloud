package eu.europeana.cloud.service.dps.storm.topologies.media.service;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.gson.Gson;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.io.ReadFileBolt;
import eu.europeana.metis.mediaprocessing.MediaExtractor;
import eu.europeana.metis.mediaprocessing.MediaProcessorFactory;
import eu.europeana.metis.mediaprocessing.RdfConverterFactory;
import eu.europeana.metis.mediaprocessing.RdfDeserializer;
import eu.europeana.metis.mediaprocessing.model.RdfResourceEntry;
import eu.europeana.metis.mediaprocessing.model.ResourceExtractionResult;
import eu.europeana.metis.mediaprocessing.model.Thumbnail;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.List;

public class EDMObjectProcessorBolt extends ReadFileBolt {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(EDMObjectProcessorBolt.class);
    private static final String MEDIA_RESOURCE_EXCEPTION = "media resource exception";

    private final AmazonClient amazonClient;

    private Gson gson;
    private MediaExtractor mediaExtractor;
    private RdfDeserializer rdfDeserializer;

    public EDMObjectProcessorBolt(String ecloudMcsAddress, AmazonClient amazonClient) {
        super(ecloudMcsAddress);
        this.amazonClient = amazonClient;
    }


    @Override
    public void execute(StormTaskTuple stormTaskTuple) {
        LOGGER.info("Starting edm:object processing");
        long processingStartTime = new Date().getTime();
        StringBuilder exception = new StringBuilder();

        try (InputStream stream = getFileStreamByStormTuple(stormTaskTuple)) {
            byte[] fileContent = IOUtils.toByteArray(stream);

            RdfResourceEntry edmObjectResourceEntry = rdfDeserializer.getMainThumbnailResourceForMediaExtraction(fileContent);
            boolean mainThumbnailAvailable = false;

            if (edmObjectResourceEntry != null) {
                ResourceExtractionResult resourceExtractionResult = mediaExtractor.performMediaExtraction(edmObjectResourceEntry, mainThumbnailAvailable);
                if (resourceExtractionResult != null) {
                    if (resourceExtractionResult.getMetadata() != null) {
                        stormTaskTuple.addParameter(PluginParameterKeys.RESOURCE_METADATA, gson.toJson(resourceExtractionResult.getMetadata()));
                        mainThumbnailAvailable = !resourceExtractionResult.getMetadata().getThumbnailTargetNames().isEmpty();
                    }

                    storeThumbnails(stormTaskTuple, exception, resourceExtractionResult);
                }
            }
            stormTaskTuple.addParameter(PluginParameterKeys.MAIN_THUMBNAIL_AVAILABLE, gson.toJson(mainThumbnailAvailable));
        } catch (Exception e) {
            LOGGER.error("Exception while reading and parsing file for processing the edm:object resource. The full error is:{} ", ExceptionUtils.getStackTrace(e));
            buildErrorMessage(exception, "Exception while processing the edm:object resource. The full error is: " + e.getMessage() + " because of: " + e.getCause());
        } finally {
            if (exception.length() > 0) {
                stormTaskTuple.addParameter(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE, exception.toString());
                stormTaskTuple.addParameter(PluginParameterKeys.UNIFIED_ERROR_MESSAGE, MEDIA_RESOURCE_EXCEPTION);
            }
            outputCollector.emit(stormTaskTuple.toStormTuple());
        }
        LOGGER.info("Processing edm:object finished in: " + (new Date().getTime() - processingStartTime) + "ms");
    }

    private void storeThumbnails(StormTaskTuple stormTaskTuple, StringBuilder exception, ResourceExtractionResult resourceExtractionResult) throws IOException {
        List<Thumbnail> thumbnails = resourceExtractionResult.getThumbnails();
        if (thumbnails != null) {
            for (Thumbnail thumbnail : thumbnails) {
                if (taskStatusChecker.hasKillFlag(stormTaskTuple.getTaskId()))
                    break;
                try (InputStream thumbnailContentStream = thumbnail.getContentStream()) {
                    amazonClient.putObject(thumbnail.getTargetName(), thumbnailContentStream, prepareObjectMetadata(thumbnail));
                } catch (Exception e) {
                    String errorMessage = "Error while uploading " + thumbnail.getTargetName() + " to S3 in Bluemix. The full error message is: " + e.getMessage() + " because of: " + e.getCause();
                    LOGGER.error(errorMessage);
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
            rdfDeserializer = new RdfConverterFactory().createRdfDeserializer();
            mediaExtractor = new MediaProcessorFactory().createMediaExtractor();
            gson = new Gson();
        } catch (Exception e) {
            LOGGER.error("Error while initialization", e);
            throw new RuntimeException(e);
        }
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
