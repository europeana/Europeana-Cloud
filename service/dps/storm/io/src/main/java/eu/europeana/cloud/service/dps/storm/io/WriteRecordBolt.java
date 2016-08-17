package eu.europeana.cloud.service.dps.storm.io;


import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.dps.States;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.TaskTupleUtility;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URI;

/**
 * Stores a Record on the cloud.
 * <p/>
 * Receives a byte array representing a Record from a tuple, creates and stores
 * a new Record on the cloud, and emits the URL of the newly created record.
 */
public class WriteRecordBolt extends AbstractDpsBolt {
    private String ecloudMcsAddress;
    public static final Logger LOGGER = LoggerFactory.getLogger(WriteRecordBolt.class);

    public WriteRecordBolt(String ecloudMcsAddress) {
        this.ecloudMcsAddress = ecloudMcsAddress;

    }

    @Override
    public void prepare() {
    }

    @Override
    public void execute(StormTaskTuple t) {
        try {
            LOGGER.info("WriteRecordBolt: persisting...");
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

    private URI uploadFileInNewRepresentation(StormTaskTuple stormTaskTuple) throws MalformedURLException, MCSException {
        FileServiceClient mcsClient = new FileServiceClient(ecloudMcsAddress);
        RecordServiceClient recordServiceClient = new RecordServiceClient(ecloudMcsAddress);
        URI newFileUri = null;
        final UrlParser urlParser = new UrlParser(stormTaskTuple.getFileUrl());
        if (urlParser.isUrlToRepresentationVersionFile()) {
            final String newRepresentationName = TaskTupleUtility.getParameterFromTuple(stormTaskTuple, PluginParameterKeys.NEW_REPRESENTATION_NAME);
            final String outputMimeType = TaskTupleUtility.getParameterFromTuple(stormTaskTuple, PluginParameterKeys.OUTPUT_MIME_TYPE);
            final String authorizationHeader = stormTaskTuple.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER);
            mcsClient.useAuthorizationHeader(authorizationHeader);
            recordServiceClient.useAuthorizationHeader(authorizationHeader);
            Representation rep = recordServiceClient.getRepresentation(urlParser.getPart(UrlPart.RECORDS), urlParser.getPart(UrlPart.REPRESENTATIONS), urlParser.getPart(UrlPart.VERSIONS));
            final String fileName = stormTaskTuple.getParameter(PluginParameterKeys.OUTPUT_FILE_NAME);
            newFileUri = recordServiceClient.createRepresentation(urlParser.getPart(UrlPart.RECORDS), newRepresentationName, rep.getDataProvider(), stormTaskTuple.getFileByteDataAsStream(), fileName, outputMimeType);
        }
        return newFileUri;
    }


    private void emitSuccessNotification(long taskId, String resource,
                                         String message, String additionalInformations, String resultResource) {
        NotificationTuple nt = NotificationTuple.prepareNotification(taskId,
                resource, States.SUCCESS, message, additionalInformations, resultResource);
        outputCollector.emit(NOTIFICATION_STREAM_NAME, nt.toStormTuple());
    }

}
