package eu.europeana.cloud.service.dps.storm.dao;

import com.datastax.driver.core.BatchStatement;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class HarvestedRecordsBatchCleaner implements AutoCloseable {

  static final int BATCH_SIZE = 1000;
  private static final Logger LOGGER = LoggerFactory.getLogger(HarvestedRecordsBatchCleaner.class);

  private final HarvestedRecordsDAO dao;
  private final String metisDatasetId;
  private final TargetIndexingDatabase targetDb;
  private final Multimap<Integer, String> recordIdsByBucketMap = ArrayListMultimap.create();
  private int cleanedCount;

  public HarvestedRecordsBatchCleaner(HarvestedRecordsDAO dao, String metisDatasetId, TargetIndexingDatabase targetDb) {
    this.dao = dao;
    this.metisDatasetId = metisDatasetId;
    this.targetDb = targetDb;
  }

  public void cleanRecord(String recordId) {
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

  public int getCleanedCount() {
    return cleanedCount;
  }

  private void saveInBatch(Collection<String> recordIds) {
    BatchStatement batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
    for (String recordId : recordIds) {
      batch.add(dao.prepareCleanIndexedColumns(metisDatasetId, recordId, targetDb));
    }
    dao.executeBatch(batch);
    cleanedCount += recordIds.size();
    LOGGER.info("Cleaned: {} date and MD5 of metisDatasetId: {}, for {} records.",
        targetDb, metisDatasetId, recordIds.size());
  }

}
