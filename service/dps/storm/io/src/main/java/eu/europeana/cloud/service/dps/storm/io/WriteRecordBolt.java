package eu.europeana.cloud.service.dps.storm.io;


import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.common.utils.Clock;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.commons.utils.DateHelper;
import eu.europeana.cloud.service.commons.utils.RetryInterruptedException;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.FileDataChecker;
import eu.europeana.cloud.service.dps.storm.utils.StormTaskTupleHelper;
import eu.europeana.cloud.service.dps.storm.utils.TaskTupleUtility;
import eu.europeana.cloud.service.dps.storm.utils.UUIDWrapper;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import lombok.Data;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URI;
import java.time.Instant;
import java.util.UUID;

import static eu.europeana.cloud.service.dps.PluginParameterKeys.CLOUD_ID;
import static eu.europeana.cloud.service.dps.PluginParameterKeys.SENT_DATE;

/**
 * Stores a Record on the cloud.
 * <p/>
 * Receives a byte array representing a Record from a tuple, creates and stores a new Record on the cloud, and emits the URL of
 * the newly created record.
 */
public class WriteRecordBolt extends AbstractDpsBolt {

  private static final long serialVersionUID = 1L;
  private static final Logger LOGGER = LoggerFactory.getLogger(WriteRecordBolt.class);
  private final String ecloudMcsAddress;
  private final String topologyUserName;
  private final String topologyUserPassword;
  private final boolean topologyCreatingNewData;
  protected transient RecordServiceClient recordServiceClient;
  protected transient UISClient uisClient;

  public WriteRecordBolt(CassandraProperties cassandraProperties, String ecloudMcsAddress,
                         String topologyUserName, String topologyUserPassword, boolean topologyCreatingNewData) {
    super(cassandraProperties);
    this.ecloudMcsAddress = ecloudMcsAddress;
    this.topologyUserName = topologyUserName;
    this.topologyUserPassword = topologyUserPassword;
    this.topologyCreatingNewData = topologyCreatingNewData;
  }

  @Override
  protected boolean ignoreDeleted() {
    return false;
  }

  @Override
  public void prepare() {
    LOGGER.debug("Preparing MCS client with the following params url={} user={}", ecloudMcsAddress, topologyUserName);
    recordServiceClient = new RecordServiceClient(ecloudMcsAddress, topologyUserName, topologyUserPassword);
  }

  /*
  * New representation should be created if:
  * - We are using revision output,
  * - We are using dataset output and record is not marked as deleted and is processed as part of specific topologies.
  * (xslt, enrichment, normalization, oai or media)
   */
  private boolean shouldNewRepresentationBeCreated(StormTaskTuple tuple) {
    if (!isRevisionProvided(tuple.getRevisionToBeApplied())) return true;

    if (tuple.isMarkedAsDeleted()) {
      return false;
    }

    return topologyCreatingNewData;
  }

  private boolean isRevisionProvided(Revision revision) {
    if (revision == null) return false;
    if (revision.getRevisionName() == null) return false;
    if (revision.getCreationTimeStamp() == null) return false;
    if (revision.getRevisionProviderId() == null) return false;
    return true;
  }


  @Override
  public void execute(Tuple anchorTuple, StormTaskTuple stormTaskTuple) {
    LOGGER.debug("WriteRecordBolt: persisting processed file");
    Instant processingStartTime = Instant.now();
    try {
      if (shouldNewRepresentationBeCreated(stormTaskTuple)) {
        RecordWriteParams writeParams = prepareWriteParameters(stormTaskTuple);
        LOGGER.debug("WriteRecordBolt: prepared write parameters: {}", writeParams);
        var uri = uploadFileInNewRepresentation(stormTaskTuple, writeParams);
        LOGGER.debug("WriteRecordBolt: file modified, new URI: {}", uri);
        LOGGER.debug("File persisted in eCloud in: {}ms", Clock.millisecondsSince(processingStartTime));
        stormTaskTuple.addParameter(PluginParameterKeys.OUTPUT_URL, uri.toString());
      } else {
        LOGGER.info("WriteRecordBolt: For this record in this execution representation creation is not needed!");
      }
      prepareEmittedTuple(stormTaskTuple);
      outputCollector.emit(anchorTuple, stormTaskTuple.toStormTuple());
      outputCollector.ack(anchorTuple);
    } catch (RetryInterruptedException e) {
      handleInterruption(e, anchorTuple);
    } catch (Exception e) {
      LOGGER.warn("Unable to process the message", e);
      StringWriter stack = new StringWriter();
      e.printStackTrace(new PrintWriter(stack));
        emitErrorNotification(anchorTuple, stormTaskTuple, "Cannot process data because: " + e.getMessage(), stack.toString());
        outputCollector.ack(anchorTuple);
    }
  }

  private String getProviderId(StormTaskTuple stormTaskTuple) throws MCSException {
    Representation rep = getRepresentation(stormTaskTuple);
    return rep.getDataProvider();
  }

  private Representation getRepresentation(StormTaskTuple stormTaskTuple) throws MCSException {
    return RetryableMethodExecutor.executeOnRest("Error while getting provider id", () ->
        recordServiceClient.getRepresentation(stormTaskTuple.getParameter(PluginParameterKeys.CLOUD_ID),
            stormTaskTuple.getParameter(PluginParameterKeys.REPRESENTATION_NAME),
            stormTaskTuple.getParameter(PluginParameterKeys.REPRESENTATION_VERSION)));
  }

  private void prepareEmittedTuple(StormTaskTuple stormTaskTuple) {
    stormTaskTuple.setFileData((byte[]) null);
    stormTaskTuple.getParameters().remove(PluginParameterKeys.CLOUD_ID);
    stormTaskTuple.getParameters().remove(PluginParameterKeys.REPRESENTATION_NAME);
    stormTaskTuple.getParameters().remove(PluginParameterKeys.REPRESENTATION_VERSION);
  }

  @Data
  static class RecordWriteParams {

    String cloudId;
    String representationName;
    String providerId;
    UUID newVersion;
    String newFileName;
    String dataSetId;
  }

  protected RecordWriteParams prepareWriteParameters(StormTaskTuple tuple)
      throws CloudException, MCSException, MalformedURLException {
    var writeParams = new RecordWriteParams();
    String cloudId = obtainCloudId(tuple);
    String providerId = obtainProviderId(tuple);
    writeParams.setCloudId(cloudId);
    writeParams.setRepresentationName(TaskTupleUtility.getParameterFromTuple(tuple, PluginParameterKeys.NEW_REPRESENTATION_NAME));
    writeParams.setProviderId(providerId);
    writeParams.setNewVersion(generateNewVersionId(tuple));
    writeParams.setNewFileName(generateNewFileName(tuple));
    writeParams.setDataSetId(StormTaskTupleHelper.extractDatasetId(tuple));
    return writeParams;
  }

  private String obtainCloudId(StormTaskTuple tuple) throws MalformedURLException {
    String cloudId;
    if (tuple.ifParametersContainsKey(PluginParameterKeys.CLOUD_ID)) {
      cloudId = tuple.getParameter(PluginParameterKeys.CLOUD_ID);
    } else {
      String fileUrl = tuple.getFileUrl();
      UrlParser urlParser = new UrlParser(fileUrl);
      if (urlParser.getPart(UrlPart.RECORDS) != null) {
        cloudId = urlParser.getPart(UrlPart.RECORDS);
      } else {
        throw new MalformedURLException("URI doesn't contain cloud Id!");
      }
    }
    tuple.addParameter(cloudId, PluginParameterKeys.CLOUD_ID);
    return cloudId;
  }

  private String obtainProviderId(StormTaskTuple tuple) throws MCSException, CloudException, MalformedURLException {
    String providerId = null;
    if (tuple.ifParametersContainsKey(PluginParameterKeys.PROVIDER_ID)) {
      providerId = tuple.getParameter(PluginParameterKeys.PROVIDER_ID);
    } else if (tuple.ifParametersContainsKey(PluginParameterKeys.OUTPUT_DATA_SETS)) {
      UrlParser parser = new UrlParser(tuple.getParameter(PluginParameterKeys.OUTPUT_DATA_SETS));
      if (parser.isUrlToDataset()) {
        providerId = parser.getPart(UrlPart.DATA_PROVIDERS);
      }
    } else if (tuple.ifParametersContainsKey(CLOUD_ID)) {
      providerId = getProviderId(tuple);
    }
    if (providerId == null) {
      throw new CloudException("Couldn't obtain provider Id!", new RuntimeException());
    }
    return providerId;
  }

  protected URI uploadFileInNewRepresentation(StormTaskTuple stormTaskTuple, RecordWriteParams writeParams) throws Exception {
    if (stormTaskTuple.isMarkedAsDeleted()) {
      return createRepresentation(writeParams);
    } else {
      return createRepresentationAndUploadFile(stormTaskTuple, writeParams);
    }
  }

  private URI createRepresentation(RecordWriteParams writeParams) throws Exception {
    LOGGER.debug("Creating empty representation for tuple that is marked as deleted");
    return RetryableMethodExecutor.executeOnRest("Error while creating representation and uploading file", () ->
        recordServiceClient.createRepresentation(writeParams.getCloudId(), writeParams.getRepresentationName(),
            writeParams.getProviderId(),
            writeParams.getNewVersion(),
            writeParams.getDataSetId(),
                true));
  }

  protected URI createRepresentationAndUploadFile(StormTaskTuple stormTaskTuple, RecordWriteParams writeParams) throws Exception {
    LOGGER.debug("Creating new representation with the following params {}", writeParams);
    if (FileDataChecker.isFileDataNullOrBlank(stormTaskTuple.getFileData())) {
      LOGGER.warn("File to be uploaded is null or blank!");
    }
    return RetryableMethodExecutor.executeOnRest("Error while creating representation and uploading file", () ->
            recordServiceClient.createRepresentation(
                    writeParams.getCloudId(), writeParams.getRepresentationName(), writeParams.getProviderId(),
                    writeParams.getNewVersion(),
                    writeParams.getDataSetId(),
                    stormTaskTuple.getFileByteDataAsStream(),
                    writeParams.getNewFileName(),
                    TaskTupleUtility.getParameterFromTuple(stormTaskTuple, PluginParameterKeys.OUTPUT_MIME_TYPE)));
  }

  protected UUID generateNewVersionId(StormTaskTuple tuple) {
    return UUIDWrapper.generateRepresentationVersion(
        DateHelper.parseISODate(tuple.getParameter(SENT_DATE)).toInstant(),
        tuple.getFileUrl());
  }

  protected String generateNewFileName(StormTaskTuple tuple) {
    String fileFromNameParameter = tuple.getParameter(PluginParameterKeys.OUTPUT_FILE_NAME);
    if (fileFromNameParameter != null) {
      return fileFromNameParameter;
    } else {
      return UUIDWrapper.generateRepresentationFileName(tuple.getFileUrl());
    }
  }

}

