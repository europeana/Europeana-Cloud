package eu.europeana.cloud.service.dps.storm.io;


import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.common.utils.Clock;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.commons.urls.DataSetUrlParser;
import eu.europeana.cloud.service.commons.utils.DateHelper;
import eu.europeana.cloud.service.commons.utils.RetryInterruptedException;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.*;
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
import java.util.Map;
import java.util.UUID;

import static eu.europeana.cloud.service.dps.PluginParameterKeys.*;

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
  protected transient RecordServiceClient recordServiceClient;

  public WriteRecordBolt(CassandraProperties cassandraProperties, String ecloudMcsAddress,
      String topologyUserName, String topologyUserPassword) {
    super(cassandraProperties);
    this.ecloudMcsAddress = ecloudMcsAddress;
    this.topologyUserName = topologyUserName;
    this.topologyUserPassword = topologyUserPassword;
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

  private boolean ifRecordShouldBeIgnored(StormTaskTuple tuple) throws MalformedURLException {
    if (!tuple.isMarkedAsDeleted()) {
      return true;
    }

    Map<String, String> recordParams = tuple.getParameters();

    if (isBlank(recordParams, REVISION_NAME)) return false;
    if (isBlank(recordParams, REVISION_PROVIDER)) return false;
    if (isBlank(recordParams, REVISION_TIMESTAMP)) return false;

    String inputDataSetId = DataSetUrlParser.parse(tuple.getFileUrl()).getId();
    String outputDataSetId = StormTaskTupleHelper.extractDatasetId(tuple);

    return inputDataSetId.equals(outputDataSetId);
  }

  private boolean isBlank(Map<String, String> map, String key) {
    return !map.containsKey(key) || map.get(key).isBlank();
  }


  @Override
  public void execute(Tuple anchorTuple, StormTaskTuple stormTaskTuple) {
    LOGGER.debug("WriteRecordBolt: persisting processed file");
    Instant processingStartTime = Instant.now();
    try {
      if (ifRecordShouldBeIgnored(stormTaskTuple)) {
        RecordWriteParams writeParams = prepareWriteParameters(stormTaskTuple);
        LOGGER.debug("WriteRecordBolt: prepared write parameters: {}", writeParams);
        var uri = uploadFileInNewRepresentation(stormTaskTuple, writeParams);
        LOGGER.debug("WriteRecordBolt: file modified, new URI: {}", uri);
        prepareEmittedTuple(stormTaskTuple, uri.toString());
        outputCollector.emit(anchorTuple, stormTaskTuple.toStormTuple());
        outputCollector.ack(anchorTuple);
        LOGGER.debug("File persisted in eCloud in: {}ms", Clock.millisecondsSince(processingStartTime));
      } else {
        LOGGER.debug("WriteRecordBolt: File not suitable to be handled");
      }
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

  private void prepareEmittedTuple(StormTaskTuple stormTaskTuple, String resultedResourceURL) {
    stormTaskTuple.addParameter(PluginParameterKeys.OUTPUT_URL, resultedResourceURL);
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
    writeParams.setCloudId(tuple.getParameter(PluginParameterKeys.CLOUD_ID));
    writeParams.setRepresentationName(TaskTupleUtility.getParameterFromTuple(tuple, PluginParameterKeys.NEW_REPRESENTATION_NAME));
    writeParams.setProviderId(getProviderId(tuple));
    writeParams.setNewVersion(generateNewVersionId(tuple));
    writeParams.setNewFileName(generateNewFileName(tuple));
    writeParams.setDataSetId(StormTaskTupleHelper.extractDatasetId(tuple));
    return writeParams;
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
            writeParams.getDataSetId()));
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

