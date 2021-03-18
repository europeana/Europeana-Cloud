package eu.europeana.cloud.service.dps.storm.utils;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;

import java.util.Date;
import java.util.Iterator;
import java.util.Optional;

public class HarvestedRecordDAO extends CassandraDAO {

    static final int OAI_ID_BUCKET_COUNT = 64;
    private static HarvestedRecordDAO instance;
    private PreparedStatement insertHarvestedRecord;
    private PreparedStatement updateIndexingDate;
    private PreparedStatement findRecord;
    private PreparedStatement findAllRecordInDataset;

    public static synchronized HarvestedRecordDAO getInstance(CassandraConnectionProvider cassandra) {
        if (instance == null) {
            instance = new HarvestedRecordDAO(cassandra);
        }
        return instance;
    }

    public HarvestedRecordDAO(CassandraConnectionProvider dbService) {
        super(dbService);
    }

    @Override
    void prepareStatements() {
        insertHarvestedRecord = dbService.getSession().prepare("INSERT INTO "
                + CassandraTablesAndColumnsNames.HARVESTED_RECORD_TABLE + "("
                + CassandraTablesAndColumnsNames.HARVESTED_RECORD_PROVIDER_ID
                + "," + CassandraTablesAndColumnsNames.HARVESTED_RECORD_DATASET_ID
                + "," + CassandraTablesAndColumnsNames.HARVESTED_RECORD_BUCKET_NUMBER
                + "," + CassandraTablesAndColumnsNames.HARVESTED_RECORD_OAI_ID
                + "," + CassandraTablesAndColumnsNames.HARVESTED_RECORD_HARVEST_DATE
                + ") VALUES(?,?,?,?,?);"
        );

        insertHarvestedRecord.setConsistencyLevel(dbService.getConsistencyLevel());

        updateIndexingDate = dbService.getSession().prepare("UPDATE "
                + CassandraTablesAndColumnsNames.HARVESTED_RECORD_TABLE
                + " SET " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_INDEXING_DATE + " = ? "
                + " WHERE " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_PROVIDER_ID + " = ? "
                + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_DATASET_ID + " = ? "
                + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_BUCKET_NUMBER + " = ? "
                + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_OAI_ID + " = ? "
        );

        updateIndexingDate.setConsistencyLevel(dbService.getConsistencyLevel());

        findRecord = dbService.getSession().prepare(
                "SELECT * FROM " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_TABLE
                        + " WHERE " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_PROVIDER_ID + " = ? "
                        + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_DATASET_ID + " = ? "
                        + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_BUCKET_NUMBER + " = ? "
                        + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_OAI_ID + " = ? "
        );

        findRecord.setConsistencyLevel(dbService.getConsistencyLevel());

        findAllRecordInDataset = dbService.getSession().prepare(
                "SELECT * FROM " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_TABLE
                        + " WHERE " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_PROVIDER_ID + " = ? "
                        + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_DATASET_ID + " = ? "
                        + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_BUCKET_NUMBER + " = ? "
        );

        findAllRecordInDataset.setConsistencyLevel(dbService.getConsistencyLevel());


    }

    public void insertHarvestedRecord(HarvestedRecord record) {
        dbService.getSession().execute(insertHarvestedRecord.bind(record.getProviderId(), record.getDatasetId(),
                oaiIdBucketNo(record.getOaiId()), record.getOaiId(), record.getHarvestDate()));
    }

    public void updateIndexingDate(String providerId, String datasetId, String oaiId, Date indexingDate) {
        dbService.getSession().execute(updateIndexingDate.bind(indexingDate, providerId, datasetId, oaiIdBucketNo(oaiId), oaiId));
    }

    public Optional<HarvestedRecord> findRecord(String providerId, String datasetId, String oaiId) {
        return Optional.ofNullable(dbService.getSession().execute(
                findRecord.bind(providerId, datasetId, oaiIdBucketNo(oaiId), oaiId))
                .one()
        ).map(this::readRecord);
    }

    public Iterator<HarvestedRecord> findDatasetRecords(String providerId, String datasetId) {
        return new BucketRecordIterator<>(OAI_ID_BUCKET_COUNT,
                (bucketNumber -> queryBucket(providerId, datasetId, bucketNumber)),
                this::readRecord);
    }

    private Iterator<Row> queryBucket(String providerId, String datasetId, Integer bucketNumber) {
        return dbService.getSession().execute(
                findAllRecordInDataset.bind(providerId, datasetId, bucketNumber))
                .iterator();
    }

    private HarvestedRecord readRecord(Row row) {
        return HarvestedRecord.builder()
                .providerId(row.getString(CassandraTablesAndColumnsNames.HARVESTED_RECORD_PROVIDER_ID))
                .datasetId(row.getString(CassandraTablesAndColumnsNames.HARVESTED_RECORD_DATASET_ID))
                .oaiId(row.getString(CassandraTablesAndColumnsNames.HARVESTED_RECORD_OAI_ID))
                .harvestDate(row.getTimestamp(CassandraTablesAndColumnsNames.HARVESTED_RECORD_HARVEST_DATE))
                .indexingDate(row.getTimestamp(CassandraTablesAndColumnsNames.HARVESTED_RECORD_INDEXING_DATE))
                .md5(row.getUUID(CassandraTablesAndColumnsNames.HARVESTED_RECORD_MD5))
                .ignored(row.getBool(CassandraTablesAndColumnsNames.HARVESTED_RECORD_IGNORED))
                .build();
    }

    private int oaiIdBucketNo(String oaiId) {
        return BucketUtils.bucketNumber(oaiId, OAI_ID_BUCKET_COUNT);
    }
}
