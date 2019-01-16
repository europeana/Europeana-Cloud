package eu.europeana.cloud.service.dps.storm.topologies.media.service;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
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

import java.io.InputStream;
import java.util.List;

/**
 * Created by Tarek on 12/11/2018.
 */
public class ResourceProcessingBolt extends AbstractDpsBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(ResourceProcessingBolt.class);
    private static final String MEDIA_RESOURCE_EXCEPTION = "media resource exception";


    static AmazonS3 amazonClient;
    private String awsAccessKey;
    private String awsSecretKey;
    private String awsEndPoint;
    private String awsBucket;


    private Gson gson;
    private MediaExtractor mediaExtractor;


    public ResourceProcessingBolt(String awsAccessKey, String awsSecretKey, String awsEndPoint, String awsBucket) {
        this.awsAccessKey = awsAccessKey;
        this.awsSecretKey = awsSecretKey;
        this.awsEndPoint = awsEndPoint;
        this.awsBucket = awsBucket;
    }


    @Override
    public void execute(StormTaskTuple stormTaskTuple) {
        StringBuilder exception = new StringBuilder();
        if (stormTaskTuple.getParameter(PluginParameterKeys.RESOURCE_LINKS_COUNT) == null) {
            outputCollector.emit(stormTaskTuple.toStormTuple());
        } else {
            try {
                RdfResourceEntry rdfResourceEntry = gson.fromJson(stormTaskTuple.getParameter(PluginParameterKeys.RESOURCE_LINK_KEY), RdfResourceEntry.class);
                ResourceExtractionResult resourceExtractionResult = mediaExtractor.performMediaExtraction(rdfResourceEntry);
                if (resourceExtractionResult != null) {
                    if (resourceExtractionResult.getMetadata() != null)
                        stormTaskTuple.addParameter(PluginParameterKeys.RESOURCE_METADATA, gson.toJson(resourceExtractionResult.getMetadata()));
                    List<Thumbnail> thumbnails = resourceExtractionResult.getThumbnails();
                    if (thumbnails != null) {
                        for (Thumbnail thumbnail : thumbnails) {
                            if (taskStatusChecker.hasKillFlag(stormTaskTuple.getTaskId()))
                                break;
                            try (InputStream stream = thumbnail.getContentStream()) {
                                amazonClient.putObject(awsBucket, thumbnail.getTargetName(), stream, null);
                                LOGGER.info("The thumbnail {} was uploaded successfully to S3 in Bluemix", thumbnail.getTargetName());
                            } catch (Exception e) {
                                String errorMessage = "Error while uploading " + thumbnail.getTargetName() + " to S3 in Bluemix. The full error message is: " + e.getMessage()+" because of: "+e.getCause();
                                LOGGER.error(errorMessage);
                                buildErrorMessage(exception, errorMessage);

                            } finally {
                                thumbnail.close();
                            }
                        }
                    }
                }
            } catch (Exception e) {
                LOGGER.error("Exception while processing the resource {}. The full error is:{} ", stormTaskTuple.getParameter(PluginParameterKeys.RESOURCE_URL), ExceptionUtils.getStackTrace(e));
                buildErrorMessage(exception, "Exception while processing the resource: " + stormTaskTuple.getParameter(PluginParameterKeys.RESOURCE_URL) + ". The full error is: " + e.getMessage()+" because of: "+e.getCause());
            } finally {
                stormTaskTuple.getParameters().remove(PluginParameterKeys.RESOURCE_LINK_KEY);
                if (exception.length() > 0) {
                    stormTaskTuple.addParameter(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE, exception.toString());
                    stormTaskTuple.addParameter(PluginParameterKeys.UNIFIED_ERROR_MESSAGE, MEDIA_RESOURCE_EXCEPTION);
                }
                outputCollector.emit(stormTaskTuple.toStormTuple());

            }
        }

    }

    @Override
    public void prepare() {
        try {
            synchronized (ResourceProcessingBolt.class) {
                initAmazonClient();
            }
            createMediaExtractor();
            initGson();
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

    private void initAmazonClient() {
        if (amazonClient == null) {
            amazonClient = new AmazonS3Client(new BasicAWSCredentials(
                    awsAccessKey,
                    awsSecretKey));
            amazonClient.setEndpoint(awsEndPoint);
        }
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
