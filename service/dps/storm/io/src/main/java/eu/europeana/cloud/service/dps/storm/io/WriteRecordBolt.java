package eu.europeana.cloud.service.dps.storm.io;


import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.utils.Clock;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.commons.utils.DateHelper;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
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
import java.net.URI;
import java.time.Instant;
import java.util.Calendar;
import java.util.UUID;

import static eu.europeana.cloud.service.dps.PluginParameterKeys.SENT_DATE;

/**
 * Stores a Record on the cloud.
 * <p/>
 * Receives a byte array representing a Record from a tuple, creates and stores
 * a new Record on the cloud, and emits the URL of the newly created record.
 */
public class WriteRecordBolt extends AbstractDpsBolt {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(WriteRecordBolt.class);
    private final String ecloudMcsAddress;
    protected transient RecordServiceClient recordServiceClient;

    public WriteRecordBolt(String ecloudMcsAddress) {
        this.ecloudMcsAddress = ecloudMcsAddress;
    }

    @Override
    public void prepare() {
        recordServiceClient = new RecordServiceClient(ecloudMcsAddress);
    }

    @Override
    public void execute(Tuple anchorTuple, StormTaskTuple stormTaskTuple) {
        LOGGER.info("WriteRecordBolt: persisting processed file");
        Instant processingStartTime = Instant.now();
        try {
            RecordWriteParams writeParams = prepareWriteParameters(stormTaskTuple);
            LOGGER.info("WriteRecordBolt: prepared write parameters: {}", writeParams);
            var uri = uploadFileInNewRepresentation(stormTaskTuple, writeParams);
            LOGGER.info("WriteRecordBolt: file modified, new URI: {}", uri);
            prepareEmittedTuple(stormTaskTuple, uri.toString());
            outputCollector.emit(anchorTuple, stormTaskTuple.toStormTuple());
        } catch (Exception e) {
            LOGGER.error("Unable to process the message", e);
            StringWriter stack = new StringWriter();
            e.printStackTrace(new PrintWriter(stack));
            emitErrorNotification(anchorTuple, stormTaskTuple.getTaskId(), stormTaskTuple.isMarkedAsDeleted(),
                    stormTaskTuple.getFileUrl(), "Cannot process data because: " + e.getMessage(), stack.toString(),
                    StormTaskTupleHelper.getRecordProcessingStartTime(stormTaskTuple));
        }
        outputCollector.ack(anchorTuple);
        LOGGER.info("File persisted in eCloud in: {}ms", Clock.millisecondsSince(processingStartTime));
    }

    private String getProviderId(StormTaskTuple stormTaskTuple) throws MCSException {
        Representation rep = getRepresentation(stormTaskTuple);
        return rep.getDataProvider();
    }

    private Representation getRepresentation(StormTaskTuple stormTaskTuple) throws MCSException {
        return RetryableMethodExecutor.executeOnRest("Error while getting provider id", () ->
                recordServiceClient.getRepresentation(stormTaskTuple.getParameter(PluginParameterKeys.CLOUD_ID),
                        stormTaskTuple.getParameter(PluginParameterKeys.REPRESENTATION_NAME),
                        stormTaskTuple.getParameter(PluginParameterKeys.REPRESENTATION_VERSION),
                        AUTHORIZATION, stormTaskTuple.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER)));
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
    }

    protected RecordWriteParams prepareWriteParameters(StormTaskTuple tuple) throws CloudException, MCSException {
        var writeParams = new RecordWriteParams();
        writeParams.setCloudId(tuple.getParameter(PluginParameterKeys.CLOUD_ID));
        writeParams.setRepresentationName(TaskTupleUtility.getParameterFromTuple(tuple, PluginParameterKeys.NEW_REPRESENTATION_NAME));
        writeParams.setProviderId(getProviderId(tuple));
        writeParams.setNewVersion(generateNewVersionId(tuple));
        writeParams.setNewFileName(generateNewFileName(tuple));
        return writeParams;
    }

    protected URI uploadFileInNewRepresentation(StormTaskTuple stormTaskTuple, RecordWriteParams writeParams) throws Exception {
        if(stormTaskTuple.isMarkedAsDeleted()){
            return createRepresentation(stormTaskTuple, writeParams);
        }else {
            return createRepresentationAndUploadFile(stormTaskTuple, writeParams);
        }
    }

    private URI createRepresentation(StormTaskTuple stormTaskTuple, RecordWriteParams writeParams) throws Exception {
        LOGGER.debug("Creating empty representation for tuple that is marked as deleted");
        return RetryableMethodExecutor.executeOnRest("Error while creating representation and uploading file", () ->
                recordServiceClient.createRepresentation(writeParams.getCloudId(), writeParams.getRepresentationName(),
                        writeParams.getProviderId(), writeParams.getNewVersion(), AUTHORIZATION,
                        stormTaskTuple.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER)));
    }

    protected URI createRepresentationAndUploadFile(StormTaskTuple stormTaskTuple, RecordWriteParams writeParams) throws Exception {
        LOGGER.debug("Creating new representation");
        return RetryableMethodExecutor.executeOnRest("Error while creating representation and uploading file", () ->
                recordServiceClient.createRepresentation(
                        writeParams.getCloudId(), writeParams.getRepresentationName(), writeParams.getProviderId(),
                        writeParams.getNewVersion(), stormTaskTuple.getFileByteDataAsStream(),
                        writeParams.getNewFileName(),
                        TaskTupleUtility.getParameterFromTuple(stormTaskTuple, PluginParameterKeys.OUTPUT_MIME_TYPE),
                        AUTHORIZATION, stormTaskTuple.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER)));
    }

    protected UUID generateNewVersionId(StormTaskTuple tuple) {
        return UUIDWrapper.generateRepresentationVersion(
                DateHelper.parseISODate(tuple.getParameter(SENT_DATE)).toInstant(),
                tuple.getFileUrl());
    }
    protected String generateNewFileName(StormTaskTuple tuple) {
        String fileFromNameParameter = tuple.getParameter(PluginParameterKeys.OUTPUT_FILE_NAME);
        if(fileFromNameParameter!=null){
            return fileFromNameParameter;
        } else {
            return UUIDWrapper.generateRepresentationFileName(tuple.getFileUrl());
        }
    }

}

