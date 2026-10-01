package eu.europeana.cloud.service.dps.storm.topologies.media.service;

import com.google.gson.Gson;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.common.utils.Clock;
import eu.europeana.cloud.service.commons.utils.RetryInterruptedException;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.TopologyGeneralException;
import eu.europeana.cloud.service.dps.storm.metric.MetricRegistry;
import eu.europeana.cloud.service.dps.storm.tuple.common.CommonTaskTuple;
import eu.europeana.metis.mediaprocessing.MediaExtractor;
import eu.europeana.metis.mediaprocessing.MediaProcessorFactory;
import eu.europeana.metis.mediaprocessing.exception.MediaProcessorException;
import eu.europeana.metis.mediaprocessing.model.RdfResourceEntry;
import eu.europeana.metis.mediaprocessing.model.ResourceExtractionResult;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;

import static eu.europeana.cloud.service.dps.storm.AbstractDpsBolt.LogStatisticsPosition.BEGIN;
import static eu.europeana.cloud.service.dps.storm.AbstractDpsBolt.LogStatisticsPosition.END;

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
  public void execute(Tuple anchorTuple, CommonTaskTuple commonTaskTuple) {
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
      RdfResourceEntry rdfResourceEntry = gson.fromJson(commonTaskTuple.getParameter(PluginParameterKeys.RESOURCE_LINK_KEY),
          RdfResourceEntry.class);
      LOGGER.info("The following resource will be processed: {}", rdfResourceEntry);
      if (rdfResourceEntry != null) {
        LOGGER.debug("Performing media extraction for: {}", rdfResourceEntry);

        long extractionStart = System.nanoTime();
        ResourceExtractionResult resourceExtractionResult =
            mediaExtractor.performMediaExtraction(
                rdfResourceEntry,
                Boolean.parseBoolean(commonTaskTuple.getParameter(PluginParameterKeys.MAIN_THUMBNAIL_AVAILABLE))
            );
        MetricRegistry.mediaRpMediaExtractionLatency(topologyName, component, (System.nanoTime() - extractionStart) / 1e9);

        if (resourceExtractionResult != null) {
          LOGGER.debug("Extracted the following metadata {}", resourceExtractionResult);
          if (resourceExtractionResult.getMetadata() != null) {
            commonTaskTuple.addParameter(PluginParameterKeys.RESOURCE_METADATA,
                gson.toJson(resourceExtractionResult.getMetadata()));
          }
          long storingStart = System.nanoTime();
          storeThumbnails(commonTaskTuple, exception, resourceExtractionResult);
          MetricRegistry.mediaRpThumbnailStoringLatency(topologyName, component, (System.nanoTime() - storingStart) / 1e9);
        }
      }
    } catch (RetryInterruptedException e) {
      handleInterruption(e, anchorTuple);
      return;
    } catch (Exception e) {
      LOGGER.error("Exception while processing the resource {}. The full error is:{} ",
          commonTaskTuple.getParameter(PluginParameterKeys.RESOURCE_URL), ExceptionUtils.getStackTrace(e));
      buildErrorMessage(exception, "Exception while processing the resource: "
          + commonTaskTuple.getParameter(PluginParameterKeys.RESOURCE_URL) + ". The full error is: "
          + e.getMessage() + " because of: " + e.getCause());
    } finally {
      logStatistics(END, STATISTIC_OPERATION_NAME, opId);
    }
    commonTaskTuple.getParameters().remove(PluginParameterKeys.RESOURCE_LINK_KEY);
    if (exception.length() > 0) {
      commonTaskTuple.addParameter(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE, exception.toString());
      commonTaskTuple.addParameter(PluginParameterKeys.UNIFIED_ERROR_MESSAGE, MEDIA_RESOURCE_EXCEPTION);
    }
    outputCollector.emit(anchorTuple, commonTaskTuple.toStormTuple());
    outputCollector.ack(anchorTuple);
    LOGGER.info("Resource processing finished in: {}ms for {}",
        Clock.millisecondsSince(processingStartTime),
        commonTaskTuple.getParameter(PluginParameterKeys.RESOURCE_URL));
  }

  private void storeThumbnails(CommonTaskTuple commonTaskTuple, StringBuilder exception,
                               ResourceExtractionResult resourceExtractionResult) throws IOException {
    thumbnailUploader.storeThumbnails(commonTaskTuple, exception, resourceExtractionResult);
  }

  @Override
  public void prepare() {
    try {
      createMediaExtractor();
      initGson();
      amazonClient.init();
      thumbnailUploader = new ThumbnailUploader(taskStatusChecker, amazonClient);
    } catch (Exception e) {
      throw new TopologyGeneralException("Error while initialization", e);
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
