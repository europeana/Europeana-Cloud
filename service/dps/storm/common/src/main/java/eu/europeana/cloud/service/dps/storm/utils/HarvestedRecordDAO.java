package eu.europeana.cloud.service.dps.storm.utils;

import com.datastax.driver.core.PreparedStatement;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;

public class HarvestedRecordDAO extends CassandraDAO {

    private static final int OAI_ID_BUCKET_COUNT = 32;
    private static HarvestedRecordDAO instance;
    private PreparedStatement insertHarvestedRecord;
    private PreparedStatement findHarvestedRecord;

    public static synchronized HarvestedRecordDAO getInstance(CassandraConnectionProvider cassandra) {
        if (instance == null) {
            instance = new HarvestedRecordDAO(cassandra);
        }
        return instance;
    }

    /**
     * @param dbService The service exposing the connection and session
     */
    public HarvestedRecordDAO(CassandraConnectionProvider dbService) {
        super(dbService);
    }

    @Override
    void prepareStatements() {
        insertHarvestedRecord = dbService.getSession().prepare("INSERT INTO "
                + CassandraTablesAndColumnsNames.HARVESTED_RECORD_TABLE + "("
                + CassandraTablesAndColumnsNames.HARVESTED_RECORD_PROVIDER_ID
                +","+ CassandraTablesAndColumnsNames.HARVESTED_RECORD_DATASET_ID
                +","+ CassandraTablesAndColumnsNames.HARVESTED_RECORD_OAI_ID_BUCKET_NO
                +","+  CassandraTablesAndColumnsNames.HARVESTED_RECORD_OAI_ID
                +","+CassandraTablesAndColumnsNames.HARVESTED_RECORD_HARVEST_DATE
                +") VALUES(?,?,?,?,?);"
                );

        insertHarvestedRecord.setConsistencyLevel(dbService.getConsistencyLevel());

        findHarvestedRecord = dbService.getSession().prepare(
                "SELECT * FROM " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_TABLE
                        + " WHERE " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_PROVIDER_ID + " = ? "
                        +                        " AND " + CassandraTablesAndColumnsNames.HARVESTED_RECORD_DATASET_ID + " = ? "
                        +                        " AND "+ CassandraTablesAndColumnsNames.HARVESTED_RECORD_OAI_ID_BUCKET_NO + " = ? "
                        +                        " AND "+ CassandraTablesAndColumnsNames.HARVESTED_RECORD_OAI_ID + " = ? ");

        findHarvestedRecord.setConsistencyLevel(dbService.getConsistencyLevel());
    }

    public void insertHarvestedRecord(HarvestedRecord record) {
        dbService.getSession().execute(insertHarvestedRecord.bind( record.getProviderId(), record.getDatasetId(),
                oaiIdBucketNo(record.getOaiId()), record.getOaiId(),record.getHarvestDate()));
    }


    private int oaiIdBucketNo(String oaiId) {
        return (OAI_ID_BUCKET_COUNT - 1) & oaiId.hashCode();
    }
}
