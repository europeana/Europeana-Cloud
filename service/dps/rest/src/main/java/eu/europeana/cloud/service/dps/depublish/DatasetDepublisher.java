package eu.europeana.cloud.service.dps.depublish;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.service.utils.indexing.IndexWrapper;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.indexing.Indexer;
import eu.europeana.indexing.exception.IndexingException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

/**
 * Service responsible for depublishing individual record or full datasets
 */
@Service
public class DatasetDepublisher {

  private final Indexer indexer;

  /**
   * Constructor.
   *
   * @param indexWrapper wrapper for {@link Indexer} implementations
   */
  public DatasetDepublisher(IndexWrapper indexWrapper) {
    this.indexer = indexWrapper.getIndexer(TargetIndexingDatabase.PUBLISH);
  }

  /**
   * Executes depublication of the whole dataset.
   *
   * @param parameters parameters describing dataset (identifier) that will be removed from the index
   * @return future containing number for removed (depublished) records
   * @throws IndexingException thrown in case of some issues with the removal
   */
  @Async
  public Future<Integer> executeDatasetDepublicationAsync(SubmitTaskParameters parameters) throws IndexingException {
    int removedCount = indexer.removeAll(parameters.getTaskParameter(PluginParameterKeys.METIS_DATASET_ID), null);
    return CompletableFuture.completedFuture(removedCount);
  }

  /**
   * Creates tombstone for the record and removes it from the index
   *
   * @param recordId record identifier to be uses for removal
   * @return if removal was successful
   * @throws IndexingException thrown in case of some issues with the removal
   */
  public boolean removeRecord(String recordId) throws IndexingException {
    boolean recordWasTombstoned = indexer.indexTombstone(recordId);
    if (recordWasTombstoned) {
      return indexer.remove(recordId);
    }
    return false;
  }

  /**
   * Counts the indexed records for the given dataset
   *
   * @param parameters parameters describing dataset (identifier) that will be counted
   * @return number of records in the dataset
   * @throws IndexingException thrown in case of some issues with the counting
   */
  public long getRecordsCount(SubmitTaskParameters parameters) throws IndexingException {
    return indexer.countRecords(parameters.getTaskParameter(PluginParameterKeys.METIS_DATASET_ID));
  }

}
