package eu.europeana.cloud.service.dps.storm.topologies.media.service;

import static eu.europeana.cloud.service.dps.storm.AbstractDpsBolt.LogStatisticsPosition.BEGIN;
import static eu.europeana.cloud.service.dps.storm.AbstractDpsBolt.LogStatisticsPosition.END;

import com.google.gson.Gson;
import com.rits.cloning.Cloner;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.common.utils.Clock;
import eu.europeana.cloud.service.commons.utils.RetryInterruptedException;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.TopologyGeneralException;
import eu.europeana.cloud.service.dps.storm.io.ReadFileBolt;
import eu.europeana.cloud.service.dps.storm.utils.FileDataChecker;
import eu.europeana.metis.mediaprocessing.MediaExtractor;
import eu.europeana.metis.mediaprocessing.MediaProcessorFactory;
import eu.europeana.metis.mediaprocessing.RdfConverterFactory;
import eu.europeana.metis.mediaprocessing.RdfDeserializer;
import eu.europeana.metis.mediaprocessing.exception.RdfDeserializationException;
import eu.europeana.metis.mediaprocessing.model.RdfResourceEntry;
import eu.europeana.metis.mediaprocessing.model.ResourceExtractionResult;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Set;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EDMObjectProcessorBolt extends ReadFileBolt {

  private static final long serialVersionUID = 1L;
  private static final Logger LOGGER = LoggerFactory.getLogger(EDMObjectProcessorBolt.class);
  private static final String MEDIA_RESOURCE_EXCEPTION = "media resource exception";
  public static final String EDM_OBJECT_ENRICHMENT_STREAM_NAME = "EdmObjectEnrichmentStream";
  private static final String STATISTIC_OPERATION_NAME = EDMObjectProcessorBolt.class.getName() + ".execute()";

  private final AmazonClient amazonClient;

  private transient Gson gson;
  private transient MediaExtractor mediaExtractor;
  private transient RdfDeserializer rdfDeserializer;
  private transient ThumbnailUploader thumbnailUploader;

  public EDMObjectProcessorBolt(CassandraProperties cassandraProperties,
      String ecloudMcsAddress,
      String ecloudMcsUser,
      String ecloudMcsUserPassword,
      AmazonClient amazonClient) {
    super(cassandraProperties, ecloudMcsAddress, ecloudMcsUser, ecloudMcsUserPassword);
    this.amazonClient = amazonClient;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    super.declareOutputFields(declarer);

    //notifications
    declarer.declareStream(EDM_OBJECT_ENRICHMENT_STREAM_NAME, StormTaskTuple.getFields());
  }

  @Override
  public void execute(Tuple anchorTuple, StormTaskTuple stormTaskTuple) {
    LOGGER.debug("Starting edm:object processing");
    // It is assigning time stamp to variable, so It has to be assigned there.
    @SuppressWarnings("java:S1941")
    var processingStartTime = Instant.now();
    var exception = new StringBuilder();
    @SuppressWarnings("java:S2245") //Random is used here only for connect begin and end log
    // operations used in statistics, and the usage is secure.
    var opId = RandomStringUtils.random(12, "0123456789abcdef");
    logStatistics(BEGIN, STATISTIC_OPERATION_NAME, opId);

    var resourcesToBeProcessed = 0;
    try (InputStream stream = getFileStreamByStormTuple(stormTaskTuple)) {
      byte[] fileContent = IOUtils.toByteArray(stream);
      if (FileDataChecker.isFileDataNullOrBlank(fileContent)) {
        LOGGER.warn("File data to be processed is null or blank!");
      }
      LOGGER.debug("Searching for main thumbnail in the resource");
      RdfResourceEntry edmObjectResourceEntry = rdfDeserializer.getMainThumbnailResourceForMediaExtraction(fileContent);
      LOGGER.info("Found the following rdfResourceEntry: {}", edmObjectResourceEntry);
      boolean mainThumbnailAvailable = false;
      // TODO Here we specify number of all resources to allow finishing task. This solution is strongly not optimal because we have
      //  to collect all the resources instead of just counting them
      resourcesToBeProcessed = rdfDeserializer.getRemainingResourcesForMediaExtraction(fileContent).size();

      if (edmObjectResourceEntry != null) {
        resourcesToBeProcessed++;
        LOGGER.debug("Performing media extraction for main thumbnails: {}", edmObjectResourceEntry);

        ResourceExtractionResult resourceExtractionResult = mediaExtractor.performMediaExtraction(edmObjectResourceEntry,
            mainThumbnailAvailable);

        if (resourceExtractionResult != null) {
          StormTaskTuple tuple = null;
          Set<String> thumbnailTargetNames = null;
          String metadataJson = null;
          if (resourceExtractionResult.getMetadata() != null) {
            tuple = new Cloner().deepClone(stormTaskTuple);
            metadataJson = gson.toJson(resourceExtractionResult.getMetadata());
            tuple.addParameter(PluginParameterKeys.RESOURCE_METADATA, metadataJson);
            thumbnailTargetNames = resourceExtractionResult.getMetadata().getThumbnailTargetNames();
            mainThumbnailAvailable = !thumbnailTargetNames.isEmpty();
            tuple.addParameter(PluginParameterKeys.RESOURCE_LINKS_COUNT, String.valueOf(resourcesToBeProcessed));
          }
          LOGGER.debug("Extracted the following metadata: thumbnailTargetNames: {},  metadata: {}",
              thumbnailTargetNames, metadataJson);
          storeThumbnails(stormTaskTuple, exception, resourceExtractionResult);
          if (tuple != null) {
            outputCollector.emit(EDM_OBJECT_ENRICHMENT_STREAM_NAME, anchorTuple, tuple.toStormTuple());
          }
        } else {
          resourcesToBeProcessed--;
          LOGGER.warn("Media extraction of main thumbnail return null.");
        }
      }
      stormTaskTuple.addParameter(PluginParameterKeys.MAIN_THUMBNAIL_AVAILABLE, gson.toJson(mainThumbnailAvailable));
    } catch (RetryInterruptedException e) {
      handleInterruption(e, anchorTuple);
      return;
    } catch (RdfDeserializationException e) {
      LOGGER.error("Unable to deserialize the file it will be dropped. The full error is:{} ", ExceptionUtils.getStackTrace(e));
        emitErrorNotification(
                anchorTuple,
                stormTaskTuple,
                "Unable to deserialize the file",
                "The cause of the error is:" + e.getCause());
    } catch (Exception e) {
      LOGGER.error("Exception while reading and parsing file for processing the edm:object resource." +
          " The full error is:{} ", ExceptionUtils.getStackTrace(e));
      StormTaskTuple tuple = new Cloner().deepClone(stormTaskTuple);
      tuple.addParameter(PluginParameterKeys.RESOURCE_LINKS_COUNT, String.valueOf(resourcesToBeProcessed));
      buildErrorMessage(exception, "Exception while processing the edm:object resource." +
          " The full error is: " + e.getMessage() + " because of: " + e.getCause());
      tuple.addParameter(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE, exception.toString());
      tuple.addParameter(PluginParameterKeys.UNIFIED_ERROR_MESSAGE, MEDIA_RESOURCE_EXCEPTION);
      outputCollector.emit(EDM_OBJECT_ENRICHMENT_STREAM_NAME, anchorTuple, tuple.toStormTuple());
    } finally {
      logStatistics(END, STATISTIC_OPERATION_NAME, opId);
    }
    if (exception.length() > 0) {
      stormTaskTuple.addParameter(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE, exception.toString());
      stormTaskTuple.addParameter(PluginParameterKeys.UNIFIED_ERROR_MESSAGE, MEDIA_RESOURCE_EXCEPTION);
    }
    stormTaskTuple.addParameter(PluginParameterKeys.RESOURCE_LINKS_COUNT, String.valueOf(resourcesToBeProcessed));
    outputCollector.emit(anchorTuple, stormTaskTuple.toStormTuple());
    outputCollector.ack(anchorTuple);

    LOGGER.info("Processing edm:object finished in: {}ms", Clock.millisecondsSince(processingStartTime));
  }

  private void storeThumbnails(StormTaskTuple stormTaskTuple, StringBuilder exception,
      ResourceExtractionResult resourceExtractionResult) throws IOException {
    thumbnailUploader.storeThumbnails(stormTaskTuple, exception, resourceExtractionResult);
  }

  @Override
  public void prepare() {
    super.prepare();
    try {
      rdfDeserializer = new RdfConverterFactory().createRdfDeserializer();
      mediaExtractor = new MediaProcessorFactory().createMediaExtractor();
      gson = new Gson();
      amazonClient.init();
      thumbnailUploader = new ThumbnailUploader(taskStatusChecker, amazonClient);
    } catch (Exception e) {
      LOGGER.error("Error while initialization", e);
      throw new TopologyGeneralException(e);
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
