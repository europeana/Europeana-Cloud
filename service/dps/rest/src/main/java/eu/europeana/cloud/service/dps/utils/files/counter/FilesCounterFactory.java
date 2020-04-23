package eu.europeana.cloud.service.dps.utils.files.counter;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.InputDataType;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
import org.springframework.stereotype.Component;

import static eu.europeana.cloud.service.dps.InputDataType.*;

/**
 * Created by Tarek on 4/6/2016.
 */
@Component
public class FilesCounterFactory {

    private CassandraTaskInfoDAO taskDAO;

    public FilesCounterFactory(CassandraTaskInfoDAO taskDAO) {
        this.taskDAO = taskDAO;
    }

    public FilesCounter createFilesCounter(DpsTask task, String topologyName) {
        String taskType = getTaskType(task);

        if (TopologiesNames.HTTP_TOPOLOGY.equals(topologyName)) {
            return new UnknownFilesNumberCounter();
        }
        if (FILE_URLS.name().equals(taskType)) {
            return new RecordFilesCounter();
        }
        if (DATASET_URLS.name().equals(taskType)) {
            return new DatasetFilesCounter(taskDAO);
        }
        if (REPOSITORY_URLS.name().equals(taskType)) {
            return new OaiPmhFilesCounter();
        } else
            return new UnknownFilesNumberCounter();
    }

    private String getTaskType(DpsTask task) {
        //TODO sholud be done in more error prone way
        final InputDataType first = task.getInputData().keySet().iterator().next();
        return first.name();
    }
}
