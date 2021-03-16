package eu.europeana.cloud.service.dps.storm.utils;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;

import java.util.Iterator;
import java.util.Optional;

public class HarvestedRecordDAO extends CassandraDAO {

    static final int OAI_ID_BUCKET_COUNT = 64;
    private static HarvestedRecordDAO instance;
    private PreparedStatement insertHarvestedRecord;
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
                + "," + CassandraTablesAndColumnsNames.HARVESTED_RECORD_OAI_ID_BUCKET_NO
                + "," + CassandraTablesAndColumnsNames.HARVESTED_RECORD_OAI_ID
                + "," + CassandraTablesAndColumnsNames.HARVESTED_RECORD_HARVEST_DATE
                + ") VALUES(?,?,?,?,?);"
        );

        insertHarvestedRecord.setConsistencyLevel(dbService.getConsistencyLevel());

        findRecord = dbService.getSession().prepare(
                "SELECT * FROM " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_TABLE
                        + " WHERE " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_PROVIDER_ID + " = ? "
                        + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_DATASET_ID + " = ? "
                        + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_OAI_ID_BUCKET_NO + " = ? "
                        + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_OAI_ID + " = ? "
        );

        findRecord.setConsistencyLevel(dbService.getConsistencyLevel());

        findAllRecordInDataset = dbService.getSession().prepare(
                "SELECT * FROM " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_TABLE
                        + " WHERE " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_PROVIDER_ID + " = ? "
                        + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_DATASET_ID + " = ? "
                        + " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_OAI_ID_BUCKET_NO + " = ? "
        );

        findAllRecordInDataset.setConsistencyLevel(dbService.getConsistencyLevel());


    }

    public void insertHarvestedRecord(HarvestedRecord record) {
        dbService.getSession().execute(insertHarvestedRecord.bind(record.getProviderId(), record.getDatasetId(),
                oaiIdBucketNo(record.getOaiId()), record.getOaiId(), record.getHarvestDate()));
    }

    public Optional<HarvestedRecord> findRecord(String providerId, String datasetId, String oaiId) {
        return Optional.ofNullable(dbService.getSession().execute(
                findRecord.bind(providerId, datasetId, oaiIdBucketNo(oaiId), oaiId))
                .one()
        ).map(this::readRecord);
    }

    public Iterator<HarvestedRecord> findDatasetRecords(String providerId, String datasetId) {
        return new HarvestedRecordIterator(providerId, datasetId);
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
        int bucketCount = OAI_ID_BUCKET_COUNT;
        return BucketUtils.bucketNumber(oaiId, bucketCount);
    }

    private class HarvestedRecordIterator extends BucketRecordIterator<HarvestedRecord> {
        private final String providerId;
        private final String datasetId;

        public HarvestedRecordIterator(String providerId, String datasetId) {
            this.providerId = providerId;
            this.datasetId = datasetId;
        }

        @Override
        protected Iterator<Row> queryBucket(int bucketNumber) {
            return dbService.getSession().execute(
                    findAllRecordInDataset.bind(providerId, datasetId, bucketNumber))
                    .iterator();
        }

        @Override
        protected HarvestedRecord convertRowToEntity(Row row) {
            return readRecord(row);
        }

    }
}
