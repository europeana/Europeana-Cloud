package eu.europeana.cloud.service.dps.utils.files.counter;

import static eu.europeana.cloud.service.dps.InputDataType.DATASET_URLS;
import static eu.europeana.cloud.service.dps.InputDataType.FILE_URLS;
import static eu.europeana.cloud.service.dps.InputDataType.REPOSITORY_URLS;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.InputDataType;
import eu.europeana.cloud.service.dps.service.utils.indexing.IndexWrapper;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
import org.springframework.stereotype.Component;

/**
 * Created by Tarek on 4/6/2016.
 */
@Component
public class FilesCounterFactory {

  private CassandraTaskInfoDAO taskInfoDAO;

  private IndexWrapper indexWrapper;

  public FilesCounterFactory(CassandraTaskInfoDAO taskInfoDAO, IndexWrapper indexWrapper) {
    this.taskInfoDAO = taskInfoDAO;
    this.indexWrapper = indexWrapper;
  }

  public FilesCounter createFilesCounter(DpsTask task, String topologyName) {
    if (TopologiesNames.HTTP_TOPOLOGY.equals(topologyName)) {
      return new UnknownFilesNumberCounter();
    }

    if (TopologiesNames.DEPUBLICATION_TOPOLOGY.equals(topologyName)) {
      return new DepublicationFilesCounter(indexWrapper);
    }

    String taskType = getTaskType(task);
    if (FILE_URLS.name().equals(taskType)) {
      return new RecordFilesCounter();
    }
    if (DATASET_URLS.name().equals(taskType)) {
      return new DatasetFilesCounter(taskInfoDAO);
    }
    if (REPOSITORY_URLS.name().equals(taskType)) {
      return new OaiPmhFilesCounter();
    } else {
      return new UnknownFilesNumberCounter();
    }
  }

  private String getTaskType(DpsTask task) {
    //TODO should be done in more error prone way
    final InputDataType first = task.getInputData().keySet().iterator().next();
    return first.name();
  }
}
