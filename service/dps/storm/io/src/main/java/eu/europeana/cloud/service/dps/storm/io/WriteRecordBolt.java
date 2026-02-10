package eu.europeana.cloud.service.dps.storm.io;


import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.common.utils.Clock;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.commons.utils.DateHelper;
import eu.europeana.cloud.service.commons.utils.RetryInterruptedException;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.*;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import lombok.Data;
import org.apache.storm.Config;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URI;
import java.time.Instant;
import java.util.*;

import static eu.europeana.cloud.service.dps.PluginParameterKeys.*;
import static eu.europeana.cloud.service.dps.storm.io.HarvestingWriteRecordBolt.ERROR_MSG_WHILE_CREATING_CLOUD_ID;
import static eu.europeana.cloud.service.dps.storm.io.HarvestingWriteRecordBolt.ERROR_MSG_WHILE_MAPPING_LOCAL_CLOUD_ID;

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
  private final String ecloudUisAddress;
  private final String topologyUserName;
  private final String topologyUserPassword;
  protected transient RecordServiceClient recordServiceClient;
  protected transient UISClient uisClient;

  public WriteRecordBolt(CassandraProperties cassandraProperties, String ecloudMcsAddress, String ecloudUisAddress,
                         String topologyUserName, String topologyUserPassword, String topologyName) {
    super(cassandraProperties);
    this.ecloudMcsAddress = ecloudMcsAddress;
    this.ecloudUisAddress = ecloudUisAddress;
    this.topologyUserName = topologyUserName;
    this.topologyUserPassword = topologyUserPassword;
    this.topologyName = topologyName;
  }

  @Override
  protected boolean ignoreDeleted() {
    return false;
  }

  @Override
  public void prepare() {
    LOGGER.debug("Preparing MCS client with the following params url={} user={}", ecloudMcsAddress, topologyUserName);
    recordServiceClient = new RecordServiceClient(ecloudMcsAddress, topologyUserName, topologyUserPassword);
    uisClient = new UISClient(ecloudUisAddress, topologyUserName, topologyUserPassword);
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

    return ifRepresentationShouldBeCreatedBasedOnTopology();
  }

  private boolean isRevisionProvided(Revision revision) {
    if (revision == null) return false;
    if (revision.getRevisionName() == null) return false;
    if (revision.getCreationTimeStamp() == null) return false;
    if (revision.getRevisionProviderId() == null) return false;
    return true;
  }

  private boolean ifRepresentationShouldBeCreatedBasedOnTopology() {
    List<String> topologiesRequiringNewRepresentation = Arrays.stream(new String[]{"xslt_topology", "enrichment_topology",
        "normalization_topology", "oai_topology", "media_topology", "http_topology"}).toList();
    return topologiesRequiringNewRepresentation.contains(this.topologyName);
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
        LOGGER.info("WriteRecordBolt: File not suitable to be handled");
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
    String providerId = obtainProviderId(tuple);
    String cloudId = obtainCloudId(tuple, providerId);
    writeParams.setCloudId(cloudId);
    writeParams.setRepresentationName(TaskTupleUtility.getParameterFromTuple(tuple, PluginParameterKeys.NEW_REPRESENTATION_NAME));
    writeParams.setProviderId(providerId);
    writeParams.setNewVersion(generateNewVersionId(tuple));
    writeParams.setNewFileName(generateNewFileName(tuple));
    writeParams.setDataSetId(StormTaskTupleHelper.extractDatasetId(tuple));
    return writeParams;
  }

  private String obtainCloudId(StormTaskTuple tuple, String providerId) throws CloudException {
    String cloudId;
    if (tuple.ifParametersContainsKey(PluginParameterKeys.CLOUD_ID)){
      cloudId = tuple.getParameter(PluginParameterKeys.CLOUD_ID);
    } else {
      String localId = tuple.getParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER);
      String additionalLocalIdentifier = tuple.getParameter(PluginParameterKeys.ADDITIONAL_LOCAL_IDENTIFIER);
      cloudId = getCloudId(providerId, localId, additionalLocalIdentifier);
    }
    return cloudId;
  }

  protected String getCloudId(String providerId, String localId, String additionalLocalIdentifier) throws CloudException {
    String result = createCloudId(providerId, localId);

    if (additionalLocalIdentifier != null) {
      attachAdditionalLocalIdentifier(additionalLocalIdentifier, result, providerId);
    }

    return result;

  }

  private void attachAdditionalLocalIdentifier(String additionalLocalIdentifier, String cloudId, String providerId)
          throws CloudException {
    RetryableMethodExecutor.executeOnRest(ERROR_MSG_WHILE_MAPPING_LOCAL_CLOUD_ID, () ->
            uisClient.createMapping(cloudId, providerId, additionalLocalIdentifier)
    );
  }

  private String createCloudId(String providerId, String localId) throws CloudException {
    return RetryableMethodExecutor.executeOnRest(ERROR_MSG_WHILE_CREATING_CLOUD_ID, () ->
            uisClient.createCloudId(providerId, localId).getId());
  }
  private String obtainProviderId(StormTaskTuple tuple) throws MCSException, CloudException {
    String providerId;
    if (tuple.ifParametersContainsKey(PluginParameterKeys.PROVIDER_ID)) {
      providerId = tuple.getParameter(PluginParameterKeys.PROVIDER_ID);
    } else if (tuple.ifParametersContainsKey(CLOUD_ID)) {
      providerId = getProviderId(tuple);
    } else {
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

