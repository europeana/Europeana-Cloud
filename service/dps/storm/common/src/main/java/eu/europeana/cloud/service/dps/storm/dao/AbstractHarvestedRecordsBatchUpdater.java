package eu.europeana.cloud.service.dps.storm.dao;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import java.util.Collection;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tool class for updating records in the harvested_records_table using batch statements.
 * Because batch statements in Cassandra are reasonable for saving to the same partition, this class
 * groups records of the same partition, the same (metis_dataset_id + bucket_number) in the map and
 * save them after collecting enough number of them or eventually during closing.
 */
public abstract class AbstractHarvestedRecordsBatchUpdater implements AutoCloseable {

  static final int BATCH_SIZE = 1000;
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractHarvestedRecordsBatchUpdater.class);
  protected final HarvestedRecordsDAO dao;
  protected final String metisDatasetId;
  protected final TargetIndexingDatabase targetDb;
  private final Multimap<Integer, String> recordIdsByBucketMap = ArrayListMultimap.create();
  @Getter
  private int cleanedCount;

  protected AbstractHarvestedRecordsBatchUpdater(HarvestedRecordsDAO dao, String metisDatasetId, TargetIndexingDatabase targetDb) {
    this.dao = dao;
    this.metisDatasetId = metisDatasetId;
    this.targetDb = targetDb;
  }

  /**
   * Executes the DB updating operation for a given recordId, the real update is done in batch, so for
   * most of the executions the records is only added to the buffer and is stored only after collecting
   * enough number of records or even eventually during closing this updater.
   *
   * @param recordId - record europeana id
   */
  public void executeRecord(String recordId) {
    int bucketNo = dao.bucketNoFor(recordId);
    recordIdsByBucketMap.put(bucketNo, recordId);
    if (recordIdsByBucketMap.get(bucketNo).size() >= BATCH_SIZE) {
      saveInBatch(recordIdsByBucketMap.removeAll(bucketNo));
    }
  }

  @Override
  public void close() {
    for (int bucketNo : recordIdsByBucketMap.keySet()) {
      saveInBatch(recordIdsByBucketMap.get(bucketNo));
    }
  }

  private void saveInBatch(Collection<String> recordIds) {
    BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
    for (String recordId : recordIds) {
      batch.add(createRequest(recordId));
    }
    dao.executeBatch(batch);
    cleanedCount += recordIds.size();
    LOGGER.info("Cleaned: {} date and MD5 of metisDatasetId: {}, for {} records.",
        targetDb, metisDatasetId, recordIds.size());
  }

  protected abstract BoundStatement createRequest(String recordId);
}
