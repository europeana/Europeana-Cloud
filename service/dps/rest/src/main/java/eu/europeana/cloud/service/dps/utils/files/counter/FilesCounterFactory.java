package eu.europeana.cloud.service.dps.utils.files.counter;

import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static eu.europeana.cloud.service.dps.InputDataType.*;

/**
 * Created by Tarek on 4/6/2016.
 */

@Component
public class FilesCounterFactory {
    @Autowired
    private TaskStatusUpdater taskStatusUpdater;

    public FilesCounter createFilesCounter(String taskType) {
        if (FILE_URLS.name().equals(taskType)) {
            return new RecordFilesCounter();
        }
        if (DATASET_URLS.name().equals(taskType)) {
            return new DatasetFilesCounter(taskStatusUpdater);
        }
        if (REPOSITORY_URLS.name().equals(taskType)) {
            return new OaiPmhFilesCounter();
        }
        else
            return null;
    }
}
