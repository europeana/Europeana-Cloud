package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.mcs.driver.RevisionServiceClient;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.mcs.exception.MCSException;

import java.net.MalformedURLException;

/**
 * Created by Tarek on 12/5/2017.
 */


public class ValidationRevisionWriter extends RevisionWriterBolt {
    public ValidationRevisionWriter(String ecloudMcsAddress) {
        super(ecloudMcsAddress);
    }

    protected void addRevisionAndEmit(StormTaskTuple stormTaskTuple, RevisionServiceClient revisionsClient) {
        LOGGER.info("{} executed",getClass().getSimpleName());
        try {
            addRevisionToSpecificResource(stormTaskTuple, revisionsClient, stormTaskTuple.getFileUrl());
            emitSuccessNotification(stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl(), "The record is validated correctly", "", "");
        } catch (MalformedURLException e) {
            LOGGER.error("URL is malformed: {}" + stormTaskTuple.getParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA));
            emitErrorNotification(stormTaskTuple.getTaskId(), null, e.getMessage(), stormTaskTuple.getParameters().toString());
        } catch (MCSException e) {
            LOGGER.warn("Error while communicating with MCS {}", e.getMessage());
            emitErrorNotification(stormTaskTuple.getTaskId(), null, e.getMessage(), stormTaskTuple.getParameters().toString());
        }
    }

}
