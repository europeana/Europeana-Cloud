package migrations.service.mcs.V10;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.*;
import eu.europeana.cloud.common.utils.Bucket;

import java.util.List;
import java.util.UUID;

public class V10_4__copy_data_to_new_tables implements JavaMigration {

    private static final int MAX_DATASET_ASSIGNMENTS_BUCKET_COUNT = 10000;

    PreparedStatement newBucketStatement;
    PreparedStatement selectAssignmentsStatement;
    PreparedStatement insertAssignmentByRepresentationsStatement;
    PreparedStatement insertAssignmentByDataSetsStatement;
    PreparedStatement updateDatasetAssignmentBuckets;
    PreparedStatement getDatasetAssignmentBucketCount;


    private void initStatements(Session session) {

        newBucketStatement = session.prepare("UPDATE data_set_assignments_by_data_set_buckets SET rows_count = rows_count + 1 WHERE provider_dataset_id = ? AND bucket_id = ?;");
        selectAssignmentsStatement = session.prepare("select provider_dataset_id,cloud_id, schema_id, version_id, creation_date from data_set_assignments");

        insertAssignmentByRepresentationsStatement = session.prepare("INSERT INTO "
                + "data_set_assignments_by_representations (cloud_id, schema_id, version_id, provider_dataset_id, creation_date) "
                + "VALUES (?,?,?,?,?);");

        insertAssignmentByDataSetsStatement = session.prepare("INSERT INTO "
                + "data_set_assignments_by_data_set (provider_dataset_id, bucket_id, schema_id, cloud_id, version_id, creation_date) "
                + "VALUES (?,?,?,?,?,?);");

        updateDatasetAssignmentBuckets = session.prepare("UPDATE data_set_assignments_by_data_set_buckets " //
                + "SET rows_count = rows_count + 1 WHERE provider_dataset_id = ? AND bucket_id = ?;");

        getDatasetAssignmentBucketCount = session.prepare("SELECT bucket_id, rows_count " //
                + "FROM data_set_assignments_by_data_set_buckets " //
                + "WHERE provider_dataset_id = ?;");
        getDatasetAssignmentBucketCount.setConsistencyLevel(ConsistencyLevel.QUORUM);

        selectAssignmentsStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);
        insertAssignmentByRepresentationsStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);
        insertAssignmentByDataSetsStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);
        updateDatasetAssignmentBuckets.setConsistencyLevel(ConsistencyLevel.QUORUM);

    }

    @Override
    public void migrate(Session session) throws Exception {
        initStatements(session);

        BoundStatement boundStatement = selectAssignmentsStatement.bind();
        boundStatement.setFetchSize(100);
        ResultSet rs = session.execute(boundStatement);
        for (Row dataset_assignment : rs) {
            //
            Bucket bucket = getCurrentDataSetAssignmentBucket(session, dataset_assignment.getString("provider_dataset_id"));
            if (bucket == null || bucket.getRowsCount() == MAX_DATASET_ASSIGNMENTS_BUCKET_COUNT) {
                bucket = new Bucket(new com.eaio.uuid.UUID().toString(), 0);
            }
            increaseDatasetAssignmentBucketCount(session, dataset_assignment.getString("provider_dataset_id"), bucket.getBucketId());
            //

            insertRowToAssignmentsByRepresentationsTable(session, dataset_assignment);
            insertRowToAssignmentsByDataSetsTable(session, bucket.getBucketId(), dataset_assignment);
        }
    }

    private Bucket getCurrentDataSetAssignmentBucket(Session session, String providerDatasetId) {
        BoundStatement statement = getDatasetAssignmentBucketCount.bind(providerDatasetId);
        ResultSet rs = session.execute(statement);

        List<Row> rows = rs.all();
        Row row = rows.isEmpty() ? null : rows.get(rows.size() - 1);
        if (row != null) {
            return new Bucket(row.getUUID("bucket_id").toString(), row.getLong("rows_count"));
        }
        return null;
    }

    private void increaseDatasetAssignmentBucketCount(Session session, String providerDataSetId, String bucketId) {
        BoundStatement statement = updateDatasetAssignmentBuckets.bind(providerDataSetId, UUID.fromString(bucketId));
        session.execute(statement);
    }

    private void insertRowToAssignmentsByRepresentationsTable(Session session, Row dataset_assignment) {
        BoundStatement boundStatement = insertAssignmentByRepresentationsStatement.bind(
                dataset_assignment.getString("cloud_id"),
                dataset_assignment.getString("schema_id"),
                UUID.fromString(dataset_assignment.getUUID("version_id").toString()),
                dataset_assignment.getString("provider_dataset_id"),
                dataset_assignment.getDate("creation_date")
        );
        session.execute(boundStatement);
    }

    private void insertRowToAssignmentsByDataSetsTable(Session session, String bucketId, Row dataset_assignment) {

        BoundStatement boundStatement = insertAssignmentByDataSetsStatement.bind(
                dataset_assignment.getString("provider_dataset_id"),
                UUID.fromString(bucketId),
                dataset_assignment.getString("schema_id"),
                dataset_assignment.getString("cloud_id"),
                dataset_assignment.getUUID("version_id"),
                dataset_assignment.getDate("creation_date")
        );
        session.execute(boundStatement);
    }
}