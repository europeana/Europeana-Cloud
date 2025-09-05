package eu.europeana.cloud.service.dps.service.utils.indexing;

import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.indexing.Indexer;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.metis.utils.DepublicationReason;

/**
 * Remove indexed record from Metis,
 */
public class IndexedRecordRemover {

  private final IndexWrapper indexWrapper;

  /**
   * Creates IndexedRecordRemover
   *
   * @param indexWrapper - Index wrapper
   */
  public IndexedRecordRemover(IndexWrapper indexWrapper) {
    this.indexWrapper = indexWrapper;
  }

  /**
   * Creates tombstone for the record and removes it from the index. The method is as a whole idempotent, so we could execute it
   * multiple time, and we always have the same final results. But this method is not atomic.
   *
   * @param targetIndexingDatabase - Metis database on which the operation is executed
   * @param recordId record identifier to be uses for removal
   * @param reason information why records was removed
   * @return if removal was successful, so the tombstone created
   * @throws IndexingException thrown in case of some issues with the removal
   */
  public boolean removeRecord(TargetIndexingDatabase targetIndexingDatabase, String recordId,
      DepublicationReason reason) throws IndexingException {
    // with these reasons, there is no tombstone created the record has to be
    // only removed
    return switch (reason) {
      case SENSITIVE_CONTENT, GDPR, PERMISSION_ISSUES -> removeRecordWithoutCreatingTombstone(targetIndexingDatabase, recordId);
      default -> createTombstoneAndRemoveRecord(targetIndexingDatabase, recordId, reason);
    };
  }

  private boolean removeRecordWithoutCreatingTombstone(TargetIndexingDatabase targetIndexingDatabase, String recordId)
      throws IndexingException {
    Indexer<FullBeanImpl> indexer = indexWrapper.getIndexer(targetIndexingDatabase);
    return indexer.remove(recordId);
  }

  private boolean createTombstoneAndRemoveRecord(TargetIndexingDatabase targetIndexingDatabase, String recordId,
      DepublicationReason reason) throws IndexingException {
    Indexer<FullBeanImpl> indexer = indexWrapper.getIndexer(targetIndexingDatabase);
    boolean recordWasTombstoned = indexer.indexTombstone(recordId, reason);

    if (recordWasTombstoned) {
      return indexer.remove(recordId);
    } else {
      boolean tombstoneExists = indexer.getTombstone(recordId) != null;

      if (tombstoneExists) {
        //If tombstone exists, it means that it was created during previous execution of the method
        // in this case we need to continue removing, because we don't know if it was completed
        // during previous execution.
        indexer.remove(recordId);
        //We always return true cause even if remove returned false it could be removed during
        //previous try, and because we checked the tombstone exists, all is ok.
        return true;
      } else {
        return false;
      }
    }
  }
}
