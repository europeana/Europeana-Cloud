package eu.europeana.cloud.service.dps.storm.dao;

import static eu.europeana.cloud.common.annotation.Retryable.DEFAULT_DELAY_BETWEEN_ATTEMPTS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyDefaultsConstants.DPS_DEFAULT_MAX_ATTEMPTS;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.annotation.Retryable;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.storm.utils.BucketRecordIterator;
import eu.europeana.cloud.service.dps.storm.utils.BucketUtils;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import java.util.Date;
import java.util.Iterator;
import java.util.Optional;
import java.util.UUID;

public class HarvestedRecordsDAO extends CassandraDAO {

  private static final int MAX_NUMBER_OF_BUCKETS = 64;
  private static final String DB_COMMUNICATION_FAILURE_MESSAGE = "Database communication failure";
  private static HarvestedRecordsDAO instance;
  private PreparedStatement insertHarvestedRecordStatement;

  private PreparedStatement updateLatestHarvestDateAndMd5Statement;
  private PreparedStatement updatePublishedHarvestDateStatement;

  private PreparedStatement findRecordStatement;
  private PreparedStatement findAllRecordInDatasetStatement;
  private PreparedStatement deleteRecordStatement;
  private PreparedStatement updatePreviewColumnsForExistingStatement;
  private PreparedStatement updatePublishedColumnsForExistingStatement;
  private PreparedStatement completePreviewColumnsIfEmptyStatement;
  private PreparedStatement completePublishedColumnsIfEmptyStatement;

  public HarvestedRecordsDAO() {
    //needed for creating cglib proxy in RetryableMethodExecutor.createRetryProxy()
  }

  public HarvestedRecordsDAO(CassandraConnectionProvider dbService) {
    super(dbService);
  }

  public static synchronized HarvestedRecordsDAO getInstance(CassandraConnectionProvider cassandra) {
    if (instance == null) {
      instance = RetryableMethodExecutor.createRetryProxy(new HarvestedRecordsDAO(cassandra));
    }
    return instance;
  }


  @Override
  protected void prepareStatements() {
    insertHarvestedRecordStatement = dbService.getSession().prepare(
        "INSERT INTO " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_TABLE
            + "("
            + CassandraTablesAndColumnsNames.HARVESTED_RECORD_METIS_DATASET_ID + ","
            + CassandraTablesAndColumnsNames.HARVESTED_RECORD_BUCKET_NUMBER + ","
            + CassandraTablesAndColumnsNames.HARVESTED_RECORD_LOCAL_ID + ","
            + CassandraTablesAndColumnsNames.HARVESTED_RECORD_LATEST_HARVEST_DATE + ","
            + CassandraTablesAndColumnsNames.HARVESTED_RECORD_LATEST_HARVEST_MD5 + ","
            + CassandraTablesAndColumnsNames.HARVESTED_RECORD_PREVIEW_HARVEST_DATE + ","
            + CassandraTablesAndColumnsNames.HARVESTED_RECORD_PREVIEW_HARVEST_MD5 + ","
            + CassandraTablesAndColumnsNames.HARVESTED_RECORD_PUBLISHED_HARVEST_DATE + ","
            + CassandraTablesAndColumnsNames.HARVESTED_RECORD_PUBLISHED_HARVEST_MD5
            + ") VALUES(?,?,?,?,?,?,?,?,?);"
    );

    updateLatestHarvestDateAndMd5Statement = dbService.getSession().prepare("UPDATE "
        + CassandraTablesAndColumnsNames.HARVESTED_RECORD_TABLE
        + " SET " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_LATEST_HARVEST_DATE + " = ? ,"
        + CassandraTablesAndColumnsNames.HARVESTED_RECORD_LATEST_HARVEST_MD5 + " = ? "
        + " WHERE " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_METIS_DATASET_ID + " = ? "
        + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_BUCKET_NUMBER + " = ? "
        + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_LOCAL_ID + " = ? "
    );

    updatePreviewColumnsForExistingStatement = dbService.getSession().prepare("UPDATE "
        + CassandraTablesAndColumnsNames.HARVESTED_RECORD_TABLE
        + " SET " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_PREVIEW_HARVEST_DATE + " = ? "
        + "," + CassandraTablesAndColumnsNames.HARVESTED_RECORD_PREVIEW_HARVEST_MD5 + " = ? "
        + " WHERE " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_METIS_DATASET_ID + " = ? "
        + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_BUCKET_NUMBER + " = ? "
        + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_LOCAL_ID + " = ? "
        + " IF EXISTS"

    );

    updatePublishedColumnsForExistingStatement = dbService.getSession().prepare("UPDATE "
        + CassandraTablesAndColumnsNames.HARVESTED_RECORD_TABLE
        + " SET " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_PUBLISHED_HARVEST_DATE + " = ? "
        + "," + CassandraTablesAndColumnsNames.HARVESTED_RECORD_PUBLISHED_HARVEST_MD5 + " = ? "
        + " WHERE " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_METIS_DATASET_ID + " = ? "
        + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_BUCKET_NUMBER + " = ? "
        + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_LOCAL_ID + " = ? "
        + " IF EXISTS"

    );

    completePreviewColumnsIfEmptyStatement = dbService.getSession().prepare("UPDATE "
        + CassandraTablesAndColumnsNames.HARVESTED_RECORD_TABLE
        + " SET " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_PREVIEW_HARVEST_DATE + " = ? "
        + "," + CassandraTablesAndColumnsNames.HARVESTED_RECORD_PREVIEW_HARVEST_MD5 + " = ? "
        + " WHERE " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_METIS_DATASET_ID + " = ? "
        + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_BUCKET_NUMBER + " = ? "
        + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_LOCAL_ID + " = ? "
        + " IF " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_PREVIEW_HARVEST_DATE + " = NULL");

    completePublishedColumnsIfEmptyStatement = dbService.getSession().prepare("UPDATE "
        + CassandraTablesAndColumnsNames.HARVESTED_RECORD_TABLE
        + " SET " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_PUBLISHED_HARVEST_DATE + " = ? "
        + "," + CassandraTablesAndColumnsNames.HARVESTED_RECORD_PUBLISHED_HARVEST_MD5 + " = ? "
        + " WHERE " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_METIS_DATASET_ID + " = ? "
        + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_BUCKET_NUMBER + " = ? "
        + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_LOCAL_ID + " = ? "
        + " IF " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_PUBLISHED_HARVEST_DATE + " = NULL");

    updatePublishedHarvestDateStatement = dbService.getSession().prepare("UPDATE "
        + CassandraTablesAndColumnsNames.HARVESTED_RECORD_TABLE
        + " SET " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_PUBLISHED_HARVEST_DATE + " = ? "
        + " WHERE " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_METIS_DATASET_ID + " = ? "
        + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_BUCKET_NUMBER + " = ? "
        + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_LOCAL_ID + " = ? "
    );

    findRecordStatement = dbService.getSession().prepare(
        "SELECT * FROM " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_TABLE
            + " WHERE " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_METIS_DATASET_ID + " = ? "
            + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_BUCKET_NUMBER + " = ? "
            + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_LOCAL_ID + " = ? "
    );

    findAllRecordInDatasetStatement = dbService.getSession().prepare(
        "SELECT * FROM " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_TABLE
            + " WHERE " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_METIS_DATASET_ID + " = ? "
            + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_BUCKET_NUMBER + " = ? "
    );

    deleteRecordStatement = dbService.getSession().prepare(
        "DELETE FROM " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_TABLE
            + " WHERE " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_METIS_DATASET_ID + " = ? "
            + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_BUCKET_NUMBER + " = ? "
            + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_LOCAL_ID + " = ? "
    );

  }

  @Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS)
  public void insertHarvestedRecord(HarvestedRecord harvestedRecord) {
    dbService.getSession().execute(prepareInsertStatement(harvestedRecord));
  }

  public BoundStatement prepareInsertStatement(HarvestedRecord harvestedRecord) {
    return insertHarvestedRecordStatement.bind(harvestedRecord.getMetisDatasetId(),
        bucketNoFor(harvestedRecord.getRecordLocalId()), harvestedRecord.getRecordLocalId(),
        harvestedRecord.getLatestHarvestDate(), harvestedRecord.getLatestHarvestMd5(),
        harvestedRecord.getPreviewHarvestDate(), harvestedRecord.getPreviewHarvestMd5(),
        harvestedRecord.getPublishedHarvestDate(), harvestedRecord.getPublishedHarvestMd5());
  }

  @Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS)
  public void updateLatestHarvestDateAndMd5(String metisDatasetId, String recordId, Date harvestDate, UUID harvestMd5) {
    dbService.getSession().execute(
        updateLatestHarvestDateAndMd5Statement.bind(harvestDate, harvestMd5, metisDatasetId, bucketNoFor(recordId), recordId));
  }

  @Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS)
  public void updatePublishedHarvestDate(String metisDatasetId, String recordId, Date indexingDate) {
    dbService.getSession()
             .execute(updatePublishedHarvestDateStatement.bind(indexingDate, metisDatasetId, bucketNoFor(recordId), recordId));
  }

  public BoundStatement createCleanIndexedColumnsStatement(String metisDatasetId, String recordId, TargetIndexingDatabase targetDb) {
    return switch (targetDb) {
      case PREVIEW -> createCleanPreviewColumnsStatement(metisDatasetId, recordId);
      case PUBLISH -> createCleanPublishedColumnsStatement(metisDatasetId, recordId);
    };
  }

  private BoundStatement createCleanPreviewColumnsStatement(String metisDatasetId, String recordId) {
    return updatePreviewColumnsForExistingStatement.bind(null, null, metisDatasetId, bucketNoFor(recordId), recordId);
  }

  private BoundStatement createCleanPublishedColumnsStatement(String metisDatasetId, String recordId) {
    return updatePublishedColumnsForExistingStatement.bind(null, null, metisDatasetId, bucketNoFor(recordId), recordId);
  }

  public BoundStatement createCompleteIndexedColumnsIfEmptyStatement(
      String metisDatasetId, String recordId, TargetIndexingDatabase targetDb, Date date, UUID md5) {
    return switch (targetDb) {
      case PREVIEW -> createCompletePreviewColumnsIfEmptyStatement(metisDatasetId, recordId, date, md5);
      case PUBLISH -> createCompletePublishedColumnsIfEmptyStatement(metisDatasetId, recordId, date, md5);
    };
  }

  private BoundStatement createCompletePreviewColumnsIfEmptyStatement(String metisDatasetId, String recordId, Date date, UUID md5) {
    return completePreviewColumnsIfEmptyStatement.bind(date, md5, metisDatasetId, bucketNoFor(recordId), recordId);
  }

  private BoundStatement createCompletePublishedColumnsIfEmptyStatement(String metisDatasetId, String recordId, Date date, UUID md5) {
    return completePublishedColumnsIfEmptyStatement.bind(date, md5, metisDatasetId, bucketNoFor(recordId), recordId);
  }

  @Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS)
  public void deleteRecord(String metisDatasetId, String recordId) {
    dbService.getSession().execute(deleteRecordStatement.bind(metisDatasetId, bucketNoFor(recordId), recordId));
  }

  @Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS)
  public Optional<HarvestedRecord> findRecord(String metisDatasetId, String recordId) {
    return Optional.ofNullable(dbService.getSession().execute(
                                            findRecordStatement.bind(metisDatasetId, bucketNoFor(recordId), recordId))
                                        .one())
                   .map(HarvestedRecord::from);
  }

  @Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS)
  public void executeBatch(BatchStatement batch) {
    dbService.getSession().execute(batch);
  }

  public Iterator<HarvestedRecord> findDatasetRecords(String metisDatasetId) {
    return new BucketRecordIterator<>(MAX_NUMBER_OF_BUCKETS,
        (bucketNumber -> queryBucket(metisDatasetId, bucketNumber)),
        HarvestedRecord::from);
  }

  private Iterator<Row> queryBucket(String metisDatasetId, Integer bucketNumber) {
    return RetryableMethodExecutor.execute(DB_COMMUNICATION_FAILURE_MESSAGE,
        DPS_DEFAULT_MAX_ATTEMPTS,
        DEFAULT_DELAY_BETWEEN_ATTEMPTS,
        () -> dbService.getSession().execute(
                           findAllRecordInDatasetStatement.bind(metisDatasetId, bucketNumber))
                       .iterator()
    );
  }

  public int bucketNoFor(String recordId) {
    return BucketUtils.bucketNumber(recordId, MAX_NUMBER_OF_BUCKETS);
  }

}
