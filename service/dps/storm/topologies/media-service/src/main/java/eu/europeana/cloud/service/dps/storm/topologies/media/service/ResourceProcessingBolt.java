package eu.europeana.cloud.service.dps.storm.topologies.media.service;

import static eu.europeana.cloud.service.dps.storm.AbstractDpsBolt.LogStatisticsPosition.BEGIN;
import static eu.europeana.cloud.service.dps.storm.AbstractDpsBolt.LogStatisticsPosition.END;

import com.google.gson.Gson;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.common.utils.Clock;
import eu.europeana.cloud.service.commons.utils.RetryInterruptedException;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.TopologyGeneralException;
import eu.europeana.metis.mediaprocessing.MediaExtractor;
import eu.europeana.metis.mediaprocessing.MediaProcessorFactory;
import eu.europeana.metis.mediaprocessing.exception.MediaProcessorException;
import eu.europeana.metis.mediaprocessing.model.RdfResourceEntry;
import eu.europeana.metis.mediaprocessing.model.ResourceExtractionResult;
import java.io.IOException;
import java.time.Instant;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResourceProcessingBolt extends AbstractDpsBolt {

  private static final long serialVersionUID = 1L;
  private static final Logger LOGGER = LoggerFactory.getLogger(ResourceProcessingBolt.class);

  private static final String MEDIA_RESOURCE_EXCEPTION = "media resource exception";

  private static final String STATISTIC_OPERATION_NAME = ResourceProcessingBolt.class.getName() + ".execute()";

  private final AmazonClient amazonClient;

  private transient Gson gson;
  private transient MediaExtractor mediaExtractor;
  private transient ThumbnailUploader thumbnailUploader;

  public ResourceProcessingBolt(CassandraProperties cassandraProperties, AmazonClient amazonClient) {
    super(cassandraProperties);
    this.amazonClient = amazonClient;
  }

  @Override
  public void execute(Tuple anchorTuple, StormTaskTuple stormTaskTuple) {
    LOGGER.info("Starting resource processing");
    // It is assigning time stamp to variable, so It has to be assigned there.
    @SuppressWarnings("java:S1941")
    Instant processingStartTime = Instant.now();
    StringBuilder exception = new StringBuilder();
    @SuppressWarnings("java:S2245") //Random is used here only for connect begin and end log
    // operations used in statistics, and the usage is secure.
    var opId = RandomStringUtils.random(12, "0123456789abcdef");
    logStatistics(BEGIN, STATISTIC_OPERATION_NAME, opId);
    try {
      RdfResourceEntry rdfResourceEntry = gson.fromJson(stormTaskTuple.getParameter(PluginParameterKeys.RESOURCE_LINK_KEY),
          RdfResourceEntry.class);
      LOGGER.info("The following resource will be processed: {}", rdfResourceEntry);
      if (rdfResourceEntry != null) {
        LOGGER.debug("Performing media extraction for: {}", rdfResourceEntry);

        ResourceExtractionResult resourceExtractionResult =
            mediaExtractor.performMediaExtraction(
                rdfResourceEntry,
                Boolean.parseBoolean(stormTaskTuple.getParameter(PluginParameterKeys.MAIN_THUMBNAIL_AVAILABLE))
            );

        if (resourceExtractionResult != null) {
          LOGGER.debug("Extracted the following metadata {}", resourceExtractionResult);
          if (resourceExtractionResult.getMetadata() != null) {
            stormTaskTuple.addParameter(PluginParameterKeys.RESOURCE_METADATA,
                gson.toJson(resourceExtractionResult.getMetadata()));
          }
          storeThumbnails(stormTaskTuple, exception, resourceExtractionResult);
        }
      }
    } catch (RetryInterruptedException e) {
      handleInterruption(e, anchorTuple);
      return;
    } catch (Exception e) {
      LOGGER.error("Exception while processing the resource {}. The full error is:{} ",
          stormTaskTuple.getParameter(PluginParameterKeys.RESOURCE_URL), ExceptionUtils.getStackTrace(e));
      buildErrorMessage(exception, "Exception while processing the resource: "
          + stormTaskTuple.getParameter(PluginParameterKeys.RESOURCE_URL) + ". The full error is: "
          + e.getMessage() + " because of: " + e.getCause());
    } finally {
      logStatistics(END, STATISTIC_OPERATION_NAME, opId);
    }
    stormTaskTuple.getParameters().remove(PluginParameterKeys.RESOURCE_LINK_KEY);
    if (exception.length() > 0) {
      stormTaskTuple.addParameter(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE, exception.toString());
      stormTaskTuple.addParameter(PluginParameterKeys.UNIFIED_ERROR_MESSAGE, MEDIA_RESOURCE_EXCEPTION);
    }
    outputCollector.emit(anchorTuple, stormTaskTuple.toStormTuple());
    outputCollector.ack(anchorTuple);
    LOGGER.info("Resource processing finished in: {}ms for {}",
        Clock.millisecondsSince(processingStartTime),
        stormTaskTuple.getParameter(PluginParameterKeys.RESOURCE_URL));
  }

  private void storeThumbnails(StormTaskTuple stormTaskTuple, StringBuilder exception,
      ResourceExtractionResult resourceExtractionResult) throws IOException {
    thumbnailUploader.storeThumbnails(stormTaskTuple, exception, resourceExtractionResult);
  }

  @Override
  public void prepare() {
    try {
      createMediaExtractor();
      initGson();
      amazonClient.init();
      thumbnailUploader = new ThumbnailUploader(taskStatusChecker, amazonClient);
    } catch (Exception e) {
      LOGGER.error("Error while initialization", e);
      throw new TopologyGeneralException(e);
    }
  }

  void initGson() {
    gson = new Gson();
  }

  private void createMediaExtractor() throws MediaProcessorException {
    mediaExtractor = new MediaProcessorFactory().createMediaExtractor();
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
