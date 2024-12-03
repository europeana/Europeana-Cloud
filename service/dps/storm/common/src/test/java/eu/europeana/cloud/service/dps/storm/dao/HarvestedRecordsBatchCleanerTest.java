package eu.europeana.cloud.service.dps.storm.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.google.common.collect.ImmutableList;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTestBase;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import eu.europeana.cloud.test.CassandraTestInstance;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;

public class HarvestedRecordsBatchCleanerTest extends CassandraTestBase {

  private static final String METIS_DATASET_ID = "114411";
  private static final Date HARVESTED_DATE = new Date(0);
  private static final Date INDEXING_DATE = new Date(1000);
  private static final UUID MD5 = UUID.randomUUID();
  private HarvestedRecordsDAO dao;

  @Before
  public void setup() {
    CassandraConnectionProvider db = CassandraConnectionProviderSingleton.getCassandraConnectionProvider(HOST,
        CassandraTestInstance.getPort(), KEYSPACE, USER,
        PASSWORD);
    HarvestedRecordsDAO rawDao = new HarvestedRecordsDAO(db);
    dao = RetryableMethodExecutor.createRetryProxy(rawDao);
  }

  @Test
  public void shouldNotInsertRecordIfItDidNotExist() {
    HarvestedRecordsBatchCleaner cleaner = new HarvestedRecordsBatchCleaner(dao, METIS_DATASET_ID,
        TargetIndexingDatabase.PREVIEW);

    cleaner.executeRecord(generateRandomRecordId());
    cleaner.close();

    assertFalse(dao.findDatasetRecords(METIS_DATASET_ID).hasNext());
  }

  @Test
  public void shouldUpdatePreviewColumnsInTheRecord() {
    String recordId = generateRandomRecordId();
    dao.insertHarvestedRecord(HarvestedRecord.builder().metisDatasetId(METIS_DATASET_ID).recordLocalId(recordId)
                                             .latestHarvestDate(HARVESTED_DATE).latestHarvestMd5(MD5)
                                             .previewHarvestDate(INDEXING_DATE)
                                             .previewHarvestMd5(MD5).build());
    HarvestedRecordsBatchCleaner cleaner = new HarvestedRecordsBatchCleaner(dao, METIS_DATASET_ID,
        TargetIndexingDatabase.PREVIEW);

    cleaner.executeRecord(recordId);
    cleaner.close();

    HarvestedRecord record = dao.findRecord(METIS_DATASET_ID, recordId).orElseThrow();
    assertNull(record.getPreviewHarvestDate());
    assertNull(record.getPreviewHarvestMd5());
  }

  @Test
  public void shouldUpdatePublishedColumnsInTheRecord() {
    String recordId = generateRandomRecordId();
    dao.insertHarvestedRecord(HarvestedRecord.builder().metisDatasetId(METIS_DATASET_ID).recordLocalId(recordId)
                                             .latestHarvestDate(HARVESTED_DATE).latestHarvestMd5(MD5)
                                             .previewHarvestDate(INDEXING_DATE)
                                             .previewHarvestMd5(MD5).publishedHarvestDate(INDEXING_DATE).publishedHarvestMd5(MD5)
                                             .build());
    HarvestedRecordsBatchCleaner cleaner = new HarvestedRecordsBatchCleaner(dao, METIS_DATASET_ID,
        TargetIndexingDatabase.PUBLISH);

    cleaner.executeRecord(recordId);
    cleaner.close();

    HarvestedRecord record = dao.findRecord(METIS_DATASET_ID, recordId).orElseThrow();
    assertNull(record.getPublishedHarvestDate());
    assertNull(record.getPublishedHarvestMd5());
  }

  @Test
  public void shouldNotSaveBeforeBatchSizeIsAchieved() {
    int recordCount = AbstractHarvestedRecordsBatchUpdater.BATCH_SIZE - 1;
    List<String> recordIds = generateRandomRecordIds(7, recordCount);
    for (String recordId : recordIds) {
      dao.insertHarvestedRecord(HarvestedRecord.builder().metisDatasetId(METIS_DATASET_ID).recordLocalId(recordId)
                                               .latestHarvestDate(HARVESTED_DATE).latestHarvestMd5(MD5)
                                               .previewHarvestDate(INDEXING_DATE)
                                               .previewHarvestMd5(MD5).build());
    }
    HarvestedRecordsBatchCleaner cleaner = new HarvestedRecordsBatchCleaner(dao, METIS_DATASET_ID,
        TargetIndexingDatabase.PREVIEW);

    for (String recordId : recordIds) {
      cleaner.executeRecord(recordId);
    }

    List<HarvestedRecord> result = ImmutableList.copyOf(dao.findDatasetRecords(METIS_DATASET_ID));
    for (HarvestedRecord record : result) {
      assertNotNull(record.getPreviewHarvestDate());
      assertNotNull(record.getPreviewHarvestMd5());
    }
  }


  @Test
  public void shouldSaveRecordsWhenBatchSizeIsAchieved() {
    int recordCount = AbstractHarvestedRecordsBatchUpdater.BATCH_SIZE;
    List<String> recordIds = generateRandomRecordIds(7, recordCount);
    for (String recordId : recordIds) {
      dao.insertHarvestedRecord(HarvestedRecord.builder().metisDatasetId(METIS_DATASET_ID).recordLocalId(recordId)
                                               .latestHarvestDate(HARVESTED_DATE).latestHarvestMd5(MD5)
                                               .previewHarvestDate(INDEXING_DATE)
                                               .previewHarvestMd5(MD5).build());
    }
    HarvestedRecordsBatchCleaner cleaner = new HarvestedRecordsBatchCleaner(dao, METIS_DATASET_ID,
        TargetIndexingDatabase.PREVIEW);

    for (String recordId : recordIds) {
      cleaner.executeRecord(recordId);
    }

    List<HarvestedRecord> result = ImmutableList.copyOf(dao.findDatasetRecords(METIS_DATASET_ID));
    assertEquals(recordCount, result.size());
    for (HarvestedRecord record : result) {
      assertNull(record.getPreviewHarvestDate());
      assertNull(record.getPreviewHarvestMd5());
    }
  }

  @Test
  public void shouldProperlySaveRecordsInManyBatchesForManyBuckets() {
    List<String> recordIds = new ArrayList<>();
    int recordInBucketCount = AbstractHarvestedRecordsBatchUpdater.BATCH_SIZE * 5 / 2;
    recordIds.addAll(generateRandomRecordIds(7, recordInBucketCount));
    recordIds.addAll(generateRandomRecordIds(25, recordInBucketCount));
    recordIds.addAll(generateRandomRecordIds(63, recordInBucketCount));
    Collections.shuffle(recordIds);
    for (String recordId : recordIds) {
      dao.insertHarvestedRecord(HarvestedRecord.builder().metisDatasetId(METIS_DATASET_ID).recordLocalId(recordId)
                                               .latestHarvestDate(HARVESTED_DATE).latestHarvestMd5(MD5)
                                               .previewHarvestDate(INDEXING_DATE)
                                               .previewHarvestMd5(MD5).build());
    }
    HarvestedRecordsBatchCleaner cleaner = new HarvestedRecordsBatchCleaner(dao, METIS_DATASET_ID,
        TargetIndexingDatabase.PREVIEW);

    for (String recordId : recordIds) {
      cleaner.executeRecord(recordId);
    }
    cleaner.close();

    List<HarvestedRecord> result = ImmutableList.copyOf(dao.findDatasetRecords(METIS_DATASET_ID));
    assertEquals(recordInBucketCount * 3, result.size());
    for (HarvestedRecord record : result) {
      assertNull(record.getPreviewHarvestDate());
      assertNull(record.getPreviewHarvestMd5());
    }
  }

  private List<String> generateRandomRecordIds(int bucketNo, int count) {
    List<String> result = new ArrayList<>();
    do {
      String recordId = generateRandomRecordId();
      if (dao.bucketNoFor(recordId) == bucketNo) {
        result.add(recordId);
      }
    } while (result.size() < count);
    return result;
  }

  private String generateRandomRecordId() {
    return RandomStringUtils.randomAlphanumeric(50);
  }

}