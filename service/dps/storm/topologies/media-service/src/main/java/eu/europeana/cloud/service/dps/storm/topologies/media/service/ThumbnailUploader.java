package eu.europeana.cloud.service.dps.storm.topologies.media.service;

import com.amazonaws.services.s3.model.ObjectMetadata;
import eu.europeana.cloud.common.utils.Clock;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.metis.mediaprocessing.model.ResourceExtractionResult;
import eu.europeana.metis.mediaprocessing.model.Thumbnail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.List;

public class ThumbnailUploader {

    private static final Logger LOGGER = LoggerFactory.getLogger(ThumbnailUploader.class);

    private final TaskStatusChecker taskStatusChecker;
    private final AmazonClient amazonClient;

    public ThumbnailUploader(TaskStatusChecker taskStatusChecker, AmazonClient amazonClient) {
        this.taskStatusChecker = taskStatusChecker;
        this.amazonClient = amazonClient;
    }

    public void storeThumbnails(StormTaskTuple stormTaskTuple, StringBuilder exception, ResourceExtractionResult resourceExtractionResult) throws IOException {
        LOGGER.info("Storing the thumbnail for resourceExtractionResult={}", resourceExtractionResult);
        Instant processingStartTime = Instant.now();
        List<Thumbnail> thumbnails = resourceExtractionResult.getThumbnails();
        if (thumbnails != null) {
            for (Thumbnail thumbnail : thumbnails) {
                if (taskStatusChecker.hasDroppedStatus(stormTaskTuple.getTaskId()))
                    break;
                try (InputStream thumbnailContentStream = thumbnail.getContentStream()) {
                    amazonClient.putObject(thumbnail.getTargetName(), thumbnailContentStream, prepareObjectMetadata(thumbnail));
                } catch (Exception e) {
                    String errorMessage = "Error while uploading " + thumbnail.getTargetName()
                            + " to S3 in Bluemix. The full error message is: " + e.getMessage()
                            + " because of: " + e.getCause();
                    LOGGER.error(errorMessage, e);
                    buildErrorMessage(exception, errorMessage);
                } finally {
                    thumbnail.close();
                }
            }
        }
        LOGGER.info("Storing the thumbnail finished in {}ms", Clock.millisecondsSince(processingStartTime));
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
