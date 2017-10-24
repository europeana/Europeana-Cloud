package migrations.service.uis.V2;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.*;
import eu.europeana.cloud.common.utils.*;

import java.util.List;
import java.util.UUID;

/**
 * @author Tarek.
 */
public class V2_5__DataFromTemporaryTable___Copier___provider_record_id_UIS implements JavaMigration {

    private static final String sourceTable = "provider_record_id_copy";
    private static final String targetTable = "provider_record_id";

    private static final int MAX_PROVIDER_RECORD_ID_BUCKET_COUNT = 10000;

    PreparedStatement updateProviderRecordIdBucketsStatement;
    PreparedStatement getProviderRecordIdBucketsCount;

    PreparedStatement selectFromTemporaryStatement;
    PreparedStatement insertToProviderRecordIdStatement;
    PreparedStatement getProviderRecordBucketCountStatement;


    private void initStatements(Session session) {

        final String selectFromTemporary = "SELECT "
                + " provider_id, record_id, cloud_id "
                + " FROM " + sourceTable + " ;\n";

        final String insertToProviderRecordId = "INSERT INTO "
                + targetTable + " (provider_id, record_id, cloud_id) "
                + "VALUES (?,?,?);\n";

        final String getProviderRecordBucketCount = "SELECT "
                + " bucket_id, rows_count "
                + " FROM provider_record_id_buckets " +
                "WHERE provider_id = ?;";


        final String updateProviderRecordIdBuckets = "UPDATE provider_record_id_buckets "
                + "SET rows_count = rows_count + 1 WHERE provider_id = ? AND bucket_id = ?;";


        selectFromTemporaryStatement = session.prepare(selectFromTemporary);
        insertToProviderRecordIdStatement = session.prepare(insertToProviderRecordId);
        getProviderRecordBucketCountStatement = session.prepare(getProviderRecordBucketCount);
        updateProviderRecordIdBucketsStatement = session.prepare(updateProviderRecordIdBuckets);
    }

    @Override
    public void migrate(Session session) {
        initStatements(session);
        BoundStatement boundStatement = selectFromTemporaryStatement.bind();
        boundStatement.setFetchSize(100);
        ResultSet rs = session.execute(boundStatement);
        for (Row providerRecordIdRow : rs) {
            Bucket bucket = getCurrentBucket(session, providerRecordIdRow.getString("provider_id"));
            if (bucket == null || bucket.getRowsCount() >= MAX_PROVIDER_RECORD_ID_BUCKET_COUNT) {
                bucket = new Bucket(providerRecordIdRow.getString("object_id"), new com.eaio.uuid.UUID().toString(), 0);

            }
            increaseProviderRecordIdBucketsCount(session, providerRecordIdRow.getString("provider_id"), bucket.getBucketId());
            insertToProviderRecordIdTable(session, bucket.getBucketId(), providerRecordIdRow);
        }
    }


    private void insertToProviderRecordIdTable(Session session, String bucketId, Row providerRecordIdRow) {
        BoundStatement boundStatement = insertToProviderRecordIdStatement.bind(
                providerRecordIdRow.getString("provider_id"),
                UUID.fromString(bucketId),
                providerRecordIdRow.getString("record_id"),
                providerRecordIdRow.getDate("cloud_id")
        );
        session.execute(boundStatement);
    }

    private void increaseProviderRecordIdBucketsCount(Session session, String providerId, String bucketId) {
        BoundStatement statement = updateProviderRecordIdBucketsStatement.bind(providerId, UUID.fromString(bucketId));
        session.execute(statement);
    }

    private Bucket getCurrentBucket(Session session, String providerId) {
        BoundStatement statement = getProviderRecordIdBucketsCount.bind(providerId);
        ResultSet rs = session.execute(statement);

        List<Row> rows = rs.all();
        Row row = rows.isEmpty() ? null : rows.get(rows.size() - 1);
        if (row != null) {
            return new Bucket(providerId, row.getUUID("bucket_id").toString(), row.getLong("rows_count"));
        }
        return null;
    }
}

