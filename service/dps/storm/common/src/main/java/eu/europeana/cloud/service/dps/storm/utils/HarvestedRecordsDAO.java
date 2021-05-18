package eu.europeana.cloud.service.dps.storm.utils;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.annotation.Retryable;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;

import java.util.Date;
import java.util.Iterator;
import java.util.Optional;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyDefaultsConstants.DPS_DEFAULT_MAX_ATTEMPTS;

public class HarvestedRecordsDAO extends CassandraDAO {

    private static final int MAX_NUMBER_OF_BUCKETS = 64;
    private static final String DB_COMMUNICATION_FAILURE_MESSAGE = "Database communication failure";
    private PreparedStatement insertHarvestedRecord;
    private PreparedStatement updateIndexingDate;
    private PreparedStatement findRecord;
    private PreparedStatement findAllRecordInDataset;
    private PreparedStatement deleteRecord;
    private PreparedStatement updateHarvestDate;

    public HarvestedRecordsDAO(){
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
                + "," + CassandraTablesAndColumnsNames.HARVESTED_RECORD_HARVEST_DATE
                + "," + CassandraTablesAndColumnsNames.HARVESTED_RECORD_MD5
                + "," + CassandraTablesAndColumnsNames.HARVESTED_RECORD_INDEXING_DATE
                + ") VALUES(?,?,?,?,?,?);"
        );

        insertHarvestedRecord.setConsistencyLevel(dbService.getConsistencyLevel());

        updateIndexingDate = dbService.getSession().prepare("UPDATE "
                + CassandraTablesAndColumnsNames.HARVESTED_RECORD_TABLE
                + " SET " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_INDEXING_DATE + " = ? "
                + " WHERE " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_METIS_DATASET_ID + " = ? "
                + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_BUCKET_NUMBER + " = ? "
                + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_LOCAL_ID + " = ? "
        );

        updateIndexingDate.setConsistencyLevel(dbService.getConsistencyLevel());

        updateHarvestDate = dbService.getSession().prepare("UPDATE "
                + CassandraTablesAndColumnsNames.HARVESTED_RECORD_TABLE
                + " SET " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_HARVEST_DATE + " = ? "
                + " WHERE " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_METIS_DATASET_ID + " = ? "
                + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_BUCKET_NUMBER + " = ? "
                + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_LOCAL_ID + " = ? "
        );

        updateHarvestDate.setConsistencyLevel(dbService.getConsistencyLevel());

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
    public void insertHarvestedRecord(HarvestedRecord record) {
        dbService.getSession().execute(insertHarvestedRecord.bind(record.getMetisDatasetId(),
                        oaiIdBucketNo(record.getRecordLocalId()), record.getRecordLocalId(), record.getHarvestDate(),
                record.getMd5(), record.getIndexingDate()));
    }

    @Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS)
    public void updateHarvestDate(String metisDatasetId, String oaiId, Date harvestDate) {
        dbService.getSession().execute(updateHarvestDate.bind(harvestDate, metisDatasetId, oaiIdBucketNo(oaiId), oaiId));
    }

    @Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS)
    public void updateIndexingDate(String metisDatasetId, String oaiId, Date indexingDate) {
        dbService.getSession().execute(updateIndexingDate.bind(indexingDate, metisDatasetId, oaiIdBucketNo(oaiId), oaiId));
    }

    @Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS)
    public void deleteRecord(String metisDatasetId, String oaiId) {
        dbService.getSession().execute(deleteRecord.bind(metisDatasetId, oaiIdBucketNo(oaiId), oaiId));
    }

    @Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS)
    public Optional<HarvestedRecord> findRecord(String metisDatasetId, String oaiId) {
        return Optional.ofNullable(dbService.getSession().execute(
                        findRecord.bind(metisDatasetId, oaiIdBucketNo(oaiId), oaiId))
                        .one())
                .map(HarvestedRecord::from);
    }

    public Iterator<HarvestedRecord> findDatasetRecords(String metisDatasetId) {
        return new BucketRecordIterator<>(MAX_NUMBER_OF_BUCKETS,
                (bucketNumber -> queryBucket(metisDatasetId, bucketNumber)),
                HarvestedRecord::from);
    }

    private Iterator<Row> queryBucket(String metisDatasetId, Integer bucketNumber) {
        return RetryableMethodExecutor.executeOnDb(DB_COMMUNICATION_FAILURE_MESSAGE,
                () -> dbService.getSession().execute(
                        findAllRecordInDataset.bind(metisDatasetId, bucketNumber))
                        .iterator()
        );
    }

    private int oaiIdBucketNo(String oaiId) {
        return BucketUtils.bucketNumber(oaiId, MAX_NUMBER_OF_BUCKETS);
    }

}
