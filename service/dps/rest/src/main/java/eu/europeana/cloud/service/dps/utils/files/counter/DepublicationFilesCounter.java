package eu.europeana.cloud.service.dps.utils.files.counter;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.service.utils.indexing.IndexWrapper;
import eu.europeana.indexing.Indexer;
import eu.europeana.indexing.exception.IndexingException;

public class DepublicationFilesCounter extends FilesCounter {

  private final Indexer indexer;

  public DepublicationFilesCounter(IndexWrapper indexWrapper) {
    this.indexer = indexWrapper.getIndexer(TargetIndexingDatabase.PUBLISH);
  }

  @Override
  public int getFilesCount(DpsTask task) throws TaskSubmissionException {
    if (task.getParameter(PluginParameterKeys.RECORD_IDS_TO_DEPUBLISH) != null) {
      return calculateRecordsNumber(task);
    } else if (task.getParameter(PluginParameterKeys.METIS_DATASET_ID) != null) {
      return calculateDatasetSize(task);
    } else {
      throw new TaskSubmissionException("Can't evaluate task expected size! Needed parameters not found in the task");
    }
  }

  private int calculateDatasetSize(DpsTask task) throws TaskSubmissionException {
    try {
      long expectedSize = indexer.countRecords(task.getParameter(PluginParameterKeys.METIS_DATASET_ID));

      if (expectedSize > Integer.MAX_VALUE) {
        throw new TaskSubmissionException(
            "There are " + expectedSize + " records in set. It exceeds Integer size and is not supported.");
      }
      return (int) expectedSize;
    } catch (IndexingException e) {
      throw new TaskSubmissionException("Can't evaluate task expected size!", e);
    }
  }

  private int calculateRecordsNumber(DpsTask task) {
    return task.getParameter(PluginParameterKeys.RECORD_IDS_TO_DEPUBLISH).split(",").length;
  }
}
