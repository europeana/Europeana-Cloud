package eu.europeana.cloud.service.dps.utils.files.counter;

import eu.europeana.cloud.service.dps.BatchInfo;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.TaskSource;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
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

    TaskSource source = task.getSource();
    if (source instanceof BatchInfo) {
      return new DatasetFilesCounter(taskInfoDAO);
    }
    if (source instanceof OAIPMHHarvestingDetails) {
      return new OaiPmhFilesCounter();
    } else {
      return new UnknownFilesNumberCounter();
    }
  }

}
