package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.mcs.exception.MCSException;

import java.net.MalformedURLException;

/**
 * Created by Tarek on 12/5/2017.
 */
public class ValidationRevisionWriter extends RevisionWriterBolt {

    private String successNotificationMessage;

    public ValidationRevisionWriter(String ecloudMcsAddress, String successNotificationMessage) {
        super(ecloudMcsAddress);
        this.successNotificationMessage = successNotificationMessage;
    }

    protected void addRevisionAndEmit(StormTaskTuple stormTaskTuple) {
        LOGGER.info("{} executed", getClass().getSimpleName());
        try {
            addRevisionToSpecificResource(stormTaskTuple, stormTaskTuple.getFileUrl());
            emitSuccessNotification(stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl(), successNotificationMessage, "", "");
        } catch (MalformedURLException e) {
            LOGGER.error("URL is malformed: {}", stormTaskTuple.getParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA));
            emitErrorNotification(stormTaskTuple.getTaskId(), null, e.getMessage(), "The cause of the error is:"+e.getCause());
        } catch (MCSException | DriverException e) {
            LOGGER.warn("Error while communicating with MCS {}", e.getMessage());
            emitErrorNotification(stormTaskTuple.getTaskId(), null, e.getMessage(), "The cause of the error is:"+e.getCause());
        }
    }

}
