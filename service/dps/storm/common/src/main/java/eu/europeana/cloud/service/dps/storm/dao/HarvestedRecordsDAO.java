package eu.europeana.cloud.service.dps.storm.dao;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.annotation.Retryable;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.dps.storm.utils.BucketRecordIterator;
import eu.europeana.cloud.service.dps.storm.utils.BucketUtils;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;

import java.util.Date;
import java.util.Iterator;
import java.util.Optional;

import static eu.europeana.cloud.common.annotation.Retryable.DEFAULT_DELAY_BETWEEN_ATTEMPTS;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyDefaultsConstants.DPS_DEFAULT_MAX_ATTEMPTS;

public class HarvestedRecordsDAO extends CassandraDAO {

    private static final int MAX_NUMBER_OF_BUCKETS = 64;
    private static final String DB_COMMUNICATION_FAILURE_MESSAGE = "Database communication failure";
    private static HarvestedRecordsDAO instance;
    private PreparedStatement insertHarvestedRecord;
    private PreparedStatement updateIndexingDate;
    private PreparedStatement findRecord;
    private PreparedStatement findAllRecordInDataset;
    private PreparedStatement deleteRecord;
    private PreparedStatement updateLatestHarvestDate;

    public static synchronized HarvestedRecordsDAO getInstance(CassandraConnectionProvider cassandra) {
        if (instance == null) {
            instance = RetryableMethodExecutor.createRetryProxy(new HarvestedRecordsDAO(cassandra));
        }
        return instance;
    }

    public HarvestedRecordsDAO(){
        //needed for creating cglib proxy in RetryableMethodExecutor.createRetryProxy()
    }

    public HarvestedRecordsDAO(CassandraConnectionProvider dbService) {
        super(dbService);
    }

    @Override
    void prepareStatements() {
        insertHarvestedRecord = dbService.getSession().prepare("INSERT INTO "
                + CassandraTablesAndColumnsNames.HARVESTED_RECORD_TABLE + "("
                + CassandraTablesAndColumnsNames.HARVESTED_RECORD_METIS_DATASET_ID
                + "," + CassandraTablesAndColumnsNames.HARVESTED_RECORD_BUCKET_NUMBER
                + "," + CassandraTablesAndColumnsNames.HARVESTED_RECORD_LOCAL_ID
                + "," + CassandraTablesAndColumnsNames.HARVESTED_RECORD_LATEST_HARVEST_DATE
                + "," + CassandraTablesAndColumnsNames.HARVESTED_RECORD_LATEST_HARVEST_MD5
                + "," + CassandraTablesAndColumnsNames.HARVESTED_RECORD_PUBLISHED_HARVEST_DATE
                + "," + CassandraTablesAndColumnsNames.HARVESTED_RECORD_PUBLISHED_HARVEST_MD5
                + ") VALUES(?,?,?,?,?,?,?);"
        );

        insertHarvestedRecord.setConsistencyLevel(dbService.getConsistencyLevel());

        updateIndexingDate = dbService.getSession().prepare("UPDATE "
                + CassandraTablesAndColumnsNames.HARVESTED_RECORD_TABLE
                + " SET " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_PUBLISHED_HARVEST_DATE + " = ? "
                + " WHERE " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_METIS_DATASET_ID + " = ? "
                + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_BUCKET_NUMBER + " = ? "
                + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_LOCAL_ID + " = ? "
        );

        updateIndexingDate.setConsistencyLevel(dbService.getConsistencyLevel());

        updateLatestHarvestDate = dbService.getSession().prepare("UPDATE "
                + CassandraTablesAndColumnsNames.HARVESTED_RECORD_TABLE
                + " SET " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_LATEST_HARVEST_DATE + " = ? "
                + " WHERE " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_METIS_DATASET_ID + " = ? "
                + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_BUCKET_NUMBER + " = ? "
                + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_LOCAL_ID + " = ? "
        );

        updateLatestHarvestDate.setConsistencyLevel(dbService.getConsistencyLevel());

        findRecord = dbService.getSession().prepare(
                "SELECT * FROM " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_TABLE
                        + " WHERE " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_METIS_DATASET_ID + " = ? "
                        + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_BUCKET_NUMBER + " = ? "
                        + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_LOCAL_ID + " = ? "
        );

        findRecord.setConsistencyLevel(dbService.getConsistencyLevel());

        findAllRecordInDataset = dbService.getSession().prepare(
                "SELECT * FROM " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_TABLE
                        + " WHERE " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_METIS_DATASET_ID + " = ? "
                        + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_BUCKET_NUMBER + " = ? "
        );

        findAllRecordInDataset.setConsistencyLevel(dbService.getConsistencyLevel());

        deleteRecord = dbService.getSession().prepare(
                "DELETE FROM " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_TABLE
                        + " WHERE " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_METIS_DATASET_ID + " = ? "
                        + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_BUCKET_NUMBER + " = ? "
                        + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_LOCAL_ID + " = ? "
        );

        deleteRecord.setConsistencyLevel(dbService.getConsistencyLevel());

    }

    @Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS)
    public void insertHarvestedRecord(HarvestedRecord harvestedRecord) {
        dbService.getSession().execute(insertHarvestedRecord.bind(harvestedRecord.getMetisDatasetId(),
                bucketNoFor(harvestedRecord.getRecordLocalId()), harvestedRecord.getRecordLocalId(), harvestedRecord.getLatestHarvestDate(),
                harvestedRecord.getLatestHarvestMd5(), harvestedRecord.getPublishedHarvestDate(), harvestedRecord.getPublishedHarvestMd5()));
    }

    @Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS)
    public void updateLatestHarvestDate(String metisDatasetId, String recordId, Date harvestDate) {
        dbService.getSession().execute(updateLatestHarvestDate.bind(harvestDate, metisDatasetId, bucketNoFor(recordId), recordId));
    }

    @Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS)
    public void updateIndexingDate(String metisDatasetId, String recordId, Date indexingDate) {
        dbService.getSession().execute(updateIndexingDate.bind(indexingDate, metisDatasetId, bucketNoFor(recordId), recordId));
    }

    @Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS)
    public void deleteRecord(String metisDatasetId, String recordId) {
        dbService.getSession().execute(deleteRecord.bind(metisDatasetId, bucketNoFor(recordId), recordId));
    }

    @Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS)
    public Optional<HarvestedRecord> findRecord(String metisDatasetId, String recordId) {
        return Optional.ofNullable(dbService.getSession().execute(
                findRecord.bind(metisDatasetId, bucketNoFor(recordId), recordId))
                .one())
                .map(HarvestedRecord::from);
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
                        findAllRecordInDataset.bind(metisDatasetId, bucketNumber))
                        .iterator()
        );
    }

    private int bucketNoFor(String recordId) {
        return BucketUtils.bucketNumber(recordId, MAX_NUMBER_OF_BUCKETS);
    }

}
