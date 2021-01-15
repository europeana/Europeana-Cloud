package eu.europeana.cloud.service.dps.storm.io;


import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.StormTaskTupleHelper;
import eu.europeana.cloud.service.dps.storm.utils.TaskTupleUtility;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import lombok.Data;
import org.apache.storm.tuple.Tuple;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.util.List;

import static eu.europeana.cloud.service.dps.storm.utils.Retriever.retryOnEcloudOnError;

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
        try {
            LOGGER.info("WriteRecordBolt: persisting processed file");
            RecordWriteParams writeParams = prepareWriteParameters(stormTaskTuple);
            if (isMessageResent(stormTaskTuple)) {
                processResentMessage(stormTaskTuple, writeParams);
            } else {
                processNewMessage(stormTaskTuple, writeParams);
            }
            outputCollector.emit(anchorTuple, stormTaskTuple.toStormTuple());
        } catch (Exception e) {
            LOGGER.error("Unable to process the message", e);
            StringWriter stack = new StringWriter();
            e.printStackTrace(new PrintWriter(stack));
            emitErrorNotification(anchorTuple, stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl(),
                    "Cannot process data because: " + e.getMessage(), stack.toString(),
                    StormTaskTupleHelper.getRecordProcessingStartTime(stormTaskTuple));
        }
        outputCollector.ack(anchorTuple);
    }

    private boolean isMessageResent(StormTaskTuple stormTaskTuple) {
        return StormTaskTupleHelper.isMessageResent(stormTaskTuple);
    }

    private void processResentMessage(StormTaskTuple tuple, RecordWriteParams writeParams) throws Exception {
        LOGGER.info("Reprocessing message that was sent again");
        List<Representation> representations = findRepresentationsWithSameRevision(tuple, writeParams);
        if (representations.isEmpty()) {
            processNewMessage(tuple, writeParams);
            return;
        }
        prepareEmittedTuple(tuple, representations.get(0).getFiles().get(0).getContentUri().toString());
    }

    private void processNewMessage(StormTaskTuple stormTaskTuple, RecordWriteParams writeParams) throws Exception {
        final URI uri = uploadFileInNewRepresentation(stormTaskTuple, writeParams);
        LOGGER.info("WriteRecordBolt: file modified, new URI: {}", uri);
        prepareEmittedTuple(stormTaskTuple, uri.toString());
    }

    private List<Representation> findRepresentationsWithSameRevision(StormTaskTuple tuple, RecordWriteParams writeParams) throws MCSException {
        return recordServiceClient.getRepresentationsByRevision(
                writeParams.getCloudId(), writeParams.getRepresentationName(),
                tuple.getRevisionToBeApplied().getRevisionName(),
                tuple.getRevisionToBeApplied().getRevisionProviderId(),
                new DateTime(tuple.getRevisionToBeApplied().getCreationTimeStamp(), DateTimeZone.UTC).toString(),
                AUTHORIZATION,
                tuple.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER));
    }

    private String getProviderId(StormTaskTuple stormTaskTuple) throws MCSException {
        Representation rep = getRepresentation(stormTaskTuple);
        return rep.getDataProvider();
    }

    private Representation getRepresentation(StormTaskTuple stormTaskTuple) throws MCSException {
        return retryOnEcloudOnError("Error while getting provider id", () ->
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
    }

    protected RecordWriteParams prepareWriteParameters(StormTaskTuple tuple) throws CloudException, MCSException {
        RecordWriteParams writeParams = new RecordWriteParams();
        writeParams.setCloudId(tuple.getParameter(PluginParameterKeys.CLOUD_ID));
        writeParams.setRepresentationName(TaskTupleUtility.getParameterFromTuple(tuple, PluginParameterKeys.NEW_REPRESENTATION_NAME));
        writeParams.setProviderId(getProviderId(tuple));
        return writeParams;
    }

    protected URI uploadFileInNewRepresentation(StormTaskTuple stormTaskTuple, RecordWriteParams writeParams) throws Exception {
        return createRepresentationAndUploadFile(stormTaskTuple, writeParams);
    }

    protected URI createRepresentationAndUploadFile(StormTaskTuple stormTaskTuple, RecordWriteParams writeParams) throws Exception {
        return retryOnEcloudOnError("Error while creating representation and uploading file", () ->
                recordServiceClient.createRepresentation(
                        writeParams.getCloudId(),
                        writeParams.getRepresentationName(),
                        writeParams.getProviderId(), stormTaskTuple.getFileByteDataAsStream(),
                        stormTaskTuple.getParameter(PluginParameterKeys.OUTPUT_FILE_NAME),
                        TaskTupleUtility.getParameterFromTuple(stormTaskTuple, PluginParameterKeys.OUTPUT_MIME_TYPE),
                        AUTHORIZATION, stormTaskTuple.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER)));
    }

}

