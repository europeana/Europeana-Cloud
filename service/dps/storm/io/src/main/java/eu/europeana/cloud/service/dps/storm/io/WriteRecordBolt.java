package eu.europeana.cloud.service.dps.storm.io;


import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.TaskTupleUtility;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;

/**
 * Stores a Record on the cloud.
 * <p/>
 * Receives a byte array representing a Record from a tuple, creates and stores
 * a new Record on the cloud, and emits the URL of the newly created record.
 */
public class WriteRecordBolt extends AbstractDpsBolt {
    private String ecloudMcsAddress;
    protected Logger LOGGER;

    public WriteRecordBolt(String ecloudMcsAddress) {
        this.ecloudMcsAddress = ecloudMcsAddress;
        LOGGER = LoggerFactory.getLogger(WriteRecordBolt.class);
    }


    @Override
    public void prepare() {
    }

    @Override
    public void execute(StormTaskTuple t) {
        try {
            LOGGER.info("WriteRecordBolt: persisting...");
            URI uri = uploadFileInNewRepresentation(t);
            LOGGER.info("WriteRecordBolt: file modified, new URI: {}", uri);
            prepareEmittedTuple(t, uri.toString());
            outputCollector.emit(t.toStormTuple());

        } catch (Exception e) {
            LOGGER.error(e.getMessage());
            StringWriter stack = new StringWriter();
            e.printStackTrace(new PrintWriter(stack));
            emitErrorNotification(t.getTaskId(), t.getFileUrl(), "Cannot process data because: " + e.getMessage(),
                    stack.toString());
            return;
        }
    }

    protected URI uploadFileInNewRepresentation(StormTaskTuple stormTaskTuple) throws IOException, MCSException, CloudException,DriverException {
        RecordServiceClient recordServiceClient = new RecordServiceClient(ecloudMcsAddress);
        final String authorizationHeader = stormTaskTuple.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER);
        recordServiceClient.useAuthorizationHeader(authorizationHeader);
        return createRepresentationAndUploadFile(stormTaskTuple, recordServiceClient);
    }


    protected URI createRepresentationAndUploadFile(StormTaskTuple stormTaskTuple, RecordServiceClient recordServiceClient) throws IOException, MCSException, CloudException, DriverException {
        int retries = DEFAULT_RETRIES;
        while (true) {
            try {
                return recordServiceClient.createRepresentation(stormTaskTuple.getParameter(PluginParameterKeys.CLOUD_ID), TaskTupleUtility.getParameterFromTuple(stormTaskTuple, PluginParameterKeys.NEW_REPRESENTATION_NAME), getProviderId(stormTaskTuple, recordServiceClient), stormTaskTuple.getFileByteDataAsStream(), stormTaskTuple.getParameter(PluginParameterKeys.OUTPUT_FILE_NAME), TaskTupleUtility.getParameterFromTuple(stormTaskTuple, PluginParameterKeys.OUTPUT_MIME_TYPE));
            } catch (MCSException | DriverException e) {
                if (retries-- > 0) {
                    LOGGER.warn("Error while creating representation and uploading file. Retries left {}", retries);
                    waitForSpecificTime();
                } else {
                    LOGGER.error("Error while creating representation and uploading file.");
                    throw e;
                }
            }
        }
    }

    private String getProviderId(StormTaskTuple stormTaskTuple, RecordServiceClient recordServiceClient) throws MCSException, DriverException {
        Representation rep = recordServiceClient.getRepresentation(stormTaskTuple.getParameter(PluginParameterKeys.CLOUD_ID), stormTaskTuple.getParameter(PluginParameterKeys.REPRESENTATION_NAME), stormTaskTuple.getParameter(PluginParameterKeys.REPRESENTATION_VERSION));
        return rep.getDataProvider();

    }

    private void prepareEmittedTuple(StormTaskTuple stormTaskTuple, String resultedResourceURL) {
        stormTaskTuple.addParameter(PluginParameterKeys.OUTPUT_URL, resultedResourceURL);
        stormTaskTuple.setFileData((byte[]) null);
        stormTaskTuple.getParameters().remove(PluginParameterKeys.CLOUD_ID);
        stormTaskTuple.getParameters().remove(PluginParameterKeys.REPRESENTATION_NAME);
        stormTaskTuple.getParameters().remove(PluginParameterKeys.REPRESENTATION_VERSION);
    }


}

