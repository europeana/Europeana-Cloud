package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.dps.States;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.TaskTupleUtility;
import eu.europeana.cloud.service.mcs.exception.MCSException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Stores a Record on the cloud.
 * <p/>
 * Receives a byte array representing a Record from a tuple, creates and stores
 * a new Record on the cloud, and emits the URL of the newly created record.
 */
public class WriteRecordBolt extends AbstractDpsBolt {
    private String ecloudMcsAddress;
    private FileServiceClient mcsClient;
    private RecordServiceClient recordServiceClient;
    public static final Logger LOGGER = LoggerFactory.getLogger(WriteRecordBolt.class);

    public WriteRecordBolt(String ecloudMcsAddress) {
        this.ecloudMcsAddress = ecloudMcsAddress;

    }

    @Override
    public void prepare() {

        mcsClient = new FileServiceClient(ecloudMcsAddress);
        recordServiceClient = new RecordServiceClient(ecloudMcsAddress);
    }

    @Override
    public void execute(StormTaskTuple t) {
        try {
            LOGGER.info("WriteRecordBolt: persisting...");
            String outputUrl = t.getParameter(PluginParameterKeys.OUTPUT_URL);
            boolean outputUrlMissing = false;

            if (outputUrl == null) {
                // in case OUTPUT_URL is not provided use a random one, using the input URL as the base
                outputUrl = t.getFileUrl();
                outputUrl = StringUtils.substringBefore(outputUrl, "/files");
                outputUrlMissing = true;

                LOGGER.info("WriteRecordBolt: OUTPUT_URL is not provided");
            }
            LOGGER.info("WriteRecordBolt: OUTPUT_URL: {}", outputUrl);
            URI uri = uploadFileInNewRepresentation(t);
            LOGGER.info("WriteRecordBolt: file modified, new URI:" + uri);

            emitSuccessNotification(t.getTaskId(), t.getFileUrl(), "", "", uri.toString());
            outputCollector.emit(inputTuple, t.toStormTuple());


        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            StringWriter stack = new StringWriter();
            e.printStackTrace(new PrintWriter(stack));
            emitErrorNotification(t.getTaskId(), t.getFileUrl(), "Cannot process data because: " + e.getMessage(),
                    stack.toString());
            return;
        }
    }

    private URI uploadFileInNewRepresentation(StormTaskTuple stormTaskTuple) throws MCSException {
        Map<String, String> urlParams = FileServiceClient.parseFileUri(stormTaskTuple.getFileUrl());
        TaskTupleUtility taskTupleUtility = new TaskTupleUtility();
        String newRepresentationName = taskTupleUtility.getParameterFromTuple(stormTaskTuple, PluginParameterKeys.NEW_REPRESENTATION_NAME);
        String outputMimeType = taskTupleUtility.getParameterFromTuple(stormTaskTuple, PluginParameterKeys.OUTPUT_MIME_TYPE);
        String authorizationHeader = stormTaskTuple.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER);
        mcsClient.useAuthorizationHeader(authorizationHeader);
        recordServiceClient.useAuthorizationHeader(authorizationHeader);
        Representation rep = recordServiceClient.getRepresentation(urlParams.get(ParamConstants.P_CLOUDID), urlParams.get(ParamConstants.P_REPRESENTATIONNAME), urlParams.get(ParamConstants.P_VER));
        URI newRepresentation = recordServiceClient.createRepresentation(urlParams.get(ParamConstants.P_CLOUDID), newRepresentationName, rep.getDataProvider());
        String newRepresentationVersion = findRepresentationVersion(newRepresentation);
        URI newFileUri;
        if (stormTaskTuple.getParameter(PluginParameterKeys.OUTPUT_FILE_NAME) != null) {
            String fileName = stormTaskTuple.getParameter(PluginParameterKeys.OUTPUT_FILE_NAME);
            newFileUri = mcsClient.uploadFile(urlParams.get(ParamConstants.P_CLOUDID), newRepresentationName, newRepresentationVersion, fileName, stormTaskTuple.getFileByteDataAsStream(), outputMimeType);
        } else
            newFileUri = mcsClient.uploadFile(newRepresentation.toString(), stormTaskTuple.getFileByteDataAsStream(), outputMimeType);

        recordServiceClient.persistRepresentation(urlParams.get(ParamConstants.P_CLOUDID), newRepresentationName, newRepresentationVersion);
        return newFileUri;
    }

    private String findRepresentationVersion(URI uri) throws MCSException {
        Pattern p = Pattern.compile(".*/records/([^/]+)/representations/([^/]+)/versions/([^/]+)");
        Matcher m = p.matcher(uri.toString());

        if (m.find()) {
            return m.group(3);
        } else {
            throw new MCSException("Unable to find representation version in representation URL");
        }
    }

    private void emitSuccessNotification(long taskId, String resource,
                                         String message, String additionalInformations, String resultResource) {
        NotificationTuple nt = NotificationTuple.prepareNotification(taskId,
                resource, States.SUCCESS, message, additionalInformations, resultResource);
        outputCollector.emit(NOTIFICATION_STREAM_NAME, nt.toStormTuple());
    }

}
