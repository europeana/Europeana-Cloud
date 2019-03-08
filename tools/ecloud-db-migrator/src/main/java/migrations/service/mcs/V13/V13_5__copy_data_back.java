package migrations.service.mcs.V13;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.*;
import eu.europeana.cloud.common.utils.Bucket;
import eu.europeana.cloud.service.commons.utils.BucketsHandler;

import java.util.Iterator;
import java.util.UUID;

import static migrations.common.TableCopier.hasNextRow;

public class V13_5__copy_data_back implements JavaMigration {

    private PreparedStatement selectStatement;
    private PreparedStatement insertStatement;

    private static final int MAX_DATASET_ASSIGNMENTS_BUCKET_COUNT = 100000;
    protected static final String CDSID_SEPARATOR = "\n";

    private void initStatements(Session session) {

        selectStatement = session.prepare("SELECT * FROM data_set_assignments_by_revision_id_temp");

        insertStatement = session.prepare("INSERT INTO "
                + "data_set_assignments_by_revision_id (provider_id,dataset_id,bucket_id,revision_provider_id,revision_name,revision_timestamp,representation_id,cloud_id,published,acceptance, mark_deleted) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?);");

        selectStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);
        insertStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);
    }

    @Override
    public void migrate(Session session) throws Exception {
        BucketsHandler bucketsHandler = new BucketsHandler(session);
        initStatements(session);

        long counter = 0;

        BoundStatement boundStatement = selectStatement.bind();
        boundStatement.setFetchSize(1000);
        ResultSet rs = session.execute(boundStatement);

        Iterator<Row> ri = rs.iterator();

        while (hasNextRow(ri)) {
            Row dataset_assignment = ri.next();
            //
            Bucket bucket = bucketsHandler.getCurrentBucket(
                    "data_set_assignments_by_revision_id_buckets",
                    createProviderDataSetId(dataset_assignment.getString("provider_id"), dataset_assignment.getString("dataset_id")));

            if (bucket == null || bucket.getRowsCount() >= MAX_DATASET_ASSIGNMENTS_BUCKET_COUNT) {
                bucket = new Bucket(
                        createProviderDataSetId(dataset_assignment.getString("provider_id"), dataset_assignment.getString("dataset_id")),
                        new com.eaio.uuid.UUID().toString(),
                        0);
            }
            bucketsHandler.increaseBucketCount("data_set_assignments_by_revision_id_buckets", bucket);
            //
            insertRowToAssignmentsByRepresentationsTable(session, dataset_assignment, bucket.getBucketId());

            if (++counter % 10000 == 0) {
                System.out.print("\rCopy table progress: " + counter);
            }
        }
    }

    private void insertRowToAssignmentsByRepresentationsTable(Session session, Row sourceRow, String bucketId) {

        BoundStatement boundStatement = insertStatement.bind(
                sourceRow.getString("provider_id"),
                sourceRow.getString("dataset_id"),
                UUID.fromString(bucketId),
                sourceRow.getString("revision_provider_id"),
                sourceRow.getString("revision_name"),
                sourceRow.getDate("revision_timestamp"),
                sourceRow.getString("representation_id"),
                sourceRow.getString("cloud_id"),
                sourceRow.getBool("published"),
                sourceRow.getBool("acceptance"),
                sourceRow.getBool("mark_deleted")
        );
        session.execute(boundStatement);
    }

    private String createProviderDataSetId(String providerId, String dataSetId) {
        return providerId + CDSID_SEPARATOR + dataSetId;
    }
}
