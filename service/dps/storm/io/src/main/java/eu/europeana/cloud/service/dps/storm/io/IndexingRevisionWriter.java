package eu.europeana.cloud.service.dps.storm.io;

import com.google.gson.Gson;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.metis.indexing.DataSetCleanerParameters;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.mcs.exception.MCSException;

import java.net.MalformedURLException;

/**
 * Created by Tarek on 9/24/2019.
 */
public class IndexingRevisionWriter extends RevisionWriterBolt {
    private static final long serialVersionUID = 1L;

    private String successNotificationMessage;

    public IndexingRevisionWriter(String ecloudMcsAddress, String successNotificationMessage) {
        super(ecloudMcsAddress);
        this.successNotificationMessage = successNotificationMessage;
    }

    protected void addRevisionAndEmit(StormTaskTuple stormTaskTuple) {
        LOGGER.info("{} executed", getClass().getSimpleName());
        try {
            addRevisionToSpecificResource(stormTaskTuple, stormTaskTuple.getFileUrl());
            emitSuccessNotificationForIndexing(stormTaskTuple.getTaskId(), new Gson().fromJson(stormTaskTuple.getParameter(PluginParameterKeys.DATA_SET_CLEANING_PARAMETERS), DataSetCleanerParameters.class),
                    stormTaskTuple.getParameter(PluginParameterKeys.DPS_URL),
                    stormTaskTuple.getFileUrl(), successNotificationMessage, "", "");
        } catch (MalformedURLException e) {
            LOGGER.error("URL is malformed: {}", stormTaskTuple.getParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA));
            emitErrorNotification(stormTaskTuple.getTaskId(), null, e.getMessage(), "The cause of the error is:" + e.getCause());
        } catch (MCSException | DriverException e) {
            LOGGER.warn("Error while communicating with MCS {}", e.getMessage());
            emitErrorNotification(stormTaskTuple.getTaskId(), null, e.getMessage(), "The cause of the error is:" + e.getCause());
        }
    }

}

