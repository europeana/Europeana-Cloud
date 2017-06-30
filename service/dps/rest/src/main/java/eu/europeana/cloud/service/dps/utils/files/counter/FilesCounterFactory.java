package eu.europeana.cloud.service.dps.utils.files.counter;

import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static eu.europeana.cloud.service.dps.InputDataType.DATASET_URLS;
import static eu.europeana.cloud.service.dps.InputDataType.FILE_URLS;

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
        if (FILE_URLS.name().equals(taskType))
            return new RecordFilesCounter();
        if (DATASET_URLS.name().equals(taskType))
            return new DatasetFilesCounter(dataSetServiceClient, recordServiceClient);
        else
            return null;
    }
}
