package eu.europeana.cloud.service.dps.utils.files.counter;

import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Created by Tarek on 4/6/2016.
 */

@Component
public class FilesCounterFactory {
    @Autowired
    DataSetServiceClient dataSetServiceClient;

    @Autowired
    RecordServiceClient recordServiceClient;

    public FilesCounter createFilesCounter(String taskType) {
        if (PluginParameterKeys.FILE_URLS.equals(taskType))
            return new RecordFilesCounter();
        if (PluginParameterKeys.DATASET_URLS.equals(taskType))
            return new DatasetFilesCounter(dataSetServiceClient, recordServiceClient);
        else
            return null;
    }
}
