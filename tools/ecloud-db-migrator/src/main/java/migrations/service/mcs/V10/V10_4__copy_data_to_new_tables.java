package migrations.service.mcs.V10;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import eu.europeana.cloud.common.utils.Bucket;
import eu.europeana.cloud.service.commons.utils.BucketsHandler;

import java.util.Iterator;
import java.util.UUID;

import static migrations.common.TableCopier.hasNextRow;

public class V10_4__copy_data_to_new_tables implements JavaMigration {

    private static final int MAX_DATASET_ASSIGNMENTS_BUCKET_COUNT = 100000;

    private static final String DATA_SET_ASSIGNMENTS_BY_DATA_SET_TABLE_NAME = "data_set_assignments_by_data_set_buckets";

    private PreparedStatement selectAssignmentsStatement;
    private PreparedStatement insertAssignmentByRepresentationsStatement;
    private PreparedStatement insertAssignmentByDataSetsStatement;

    private void initStatements(Session session) {

        selectAssignmentsStatement = session.prepare("select provider_dataset_id,cloud_id, schema_id, version_id, creation_date from data_set_assignments");

        insertAssignmentByRepresentationsStatement = session.prepare("INSERT INTO "
                + "data_set_assignments_by_representations (cloud_id, schema_id, version_id, provider_dataset_id, creation_date) "
                + "VALUES (?,?,?,?,?);");

        insertAssignmentByDataSetsStatement = session.prepare("INSERT INTO "
                + "data_set_assignments_by_data_set (provider_dataset_id, bucket_id, schema_id, cloud_id, version_id, creation_date) "
                + "VALUES (?,?,?,?,?,?);");

        selectAssignmentsStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);
        insertAssignmentByRepresentationsStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);
        insertAssignmentByDataSetsStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);

    }

    @Override
    public void migrate(Session session) throws Exception {
        BucketsHandler bucketsHandler = new BucketsHandler(session);
        initStatements(session);

        long counter = 0;

        BoundStatement boundStatement = selectAssignmentsStatement.bind();
        boundStatement.setFetchSize(100);
        ResultSet rs = session.execute(boundStatement);

        Iterator<Row> ri = rs.iterator();

        while (hasNextRow(ri)) {
            Row dataset_assignment = ri.next();
            //
            Bucket bucket = bucketsHandler.getCurrentBucket(DATA_SET_ASSIGNMENTS_BY_DATA_SET_TABLE_NAME, dataset_assignment.getString("provider_dataset_id"));
            if (bucket == null || bucket.getRowsCount() == MAX_DATASET_ASSIGNMENTS_BUCKET_COUNT) {
                bucket = new Bucket(dataset_assignment.getString("provider_dataset_id"), new com.eaio.uuid.UUID().toString(), 0);
            }
            bucketsHandler.increaseBucketCount(DATA_SET_ASSIGNMENTS_BY_DATA_SET_TABLE_NAME, bucket);
            //
            insertRowToAssignmentsByRepresentationsTable(session, dataset_assignment);
            insertRowToAssignmentsByDataSetsTable(session, bucket.getBucketId(), dataset_assignment);
            if (++counter % 10000 == 0) {
                System.out.print("\rCopy table progress: " + counter);
            }
        }
        if (counter > 0) {
            System.out.println("\rCopy table progress: " + counter);
        }
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