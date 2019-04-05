package migrations.service.mcs.V15;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.*;
import eu.europeana.cloud.common.utils.Bucket;
import eu.europeana.cloud.service.commons.utils.BucketsHandler;

import java.util.Iterator;
import java.util.UUID;

import static migrations.common.TableCopier.hasNextRow;

public class V15_1__copy_data implements JavaMigration {

    private PreparedStatement selectStatement;
    private PreparedStatement insertStatement;


    private PreparedStatement bucketsForSpecificObjectIdStatement;
    private PreparedStatement countOfRowsStatement;


    private static final int MAX_DATASET_ASSIGNMENTS_BY_REVISION_ID_BUCKET_COUNT = 250000;
    protected static final String CDSID_SEPARATOR = "\n";

    private void initStatements(Session session) {

        selectStatement = session.prepare("SELECT * FROM data_set_assignments_by_revision_id_replica");
        insertStatement = session.prepare("INSERT INTO "
                + "data_set_assignments_by_revision_id_v1 (provider_id,dataset_id,bucket_id,revision_provider_id,revision_name,revision_timestamp,representation_id,cloud_id,published,acceptance, mark_deleted) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?);");


        bucketsForSpecificObjectIdStatement = session.prepare("Select bucket_id from data_set_assignments_by_revision_id_buckets where object_id=?");

        countOfRowsStatement = session.prepare("Select count(*) from data_set_assignments_by_revision_id_v1 where provider_id=? and dataset_id=? and bucket_id=? and revision_provider_id=? and revision_name=? and revision_timestamp=? and representation_id=? and cloud_id=? ;");


        selectStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);
        insertStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);

        bucketsForSpecificObjectIdStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);
        countOfRowsStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);
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
            if (getRowCount(session, dataset_assignment) == 0) {
                //
                Bucket bucket = bucketsHandler.getCurrentBucket(
                        "data_set_assignments_by_revision_id_buckets",
                        createProviderDataSetId(dataset_assignment.getString("provider_id"), dataset_assignment.getString("dataset_id")));

                if (bucket == null || bucket.getRowsCount() >= MAX_DATASET_ASSIGNMENTS_BY_REVISION_ID_BUCKET_COUNT) {
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
    }

    private void insertRowToAssignmentsByRepresentationsTable(Session session, Row sourceRow, String bucketId) {
        //select

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


    private long getRowCount(Session session, Row dataset_assignment) {

        String objectId = createProviderDataSetId(dataset_assignment.getString("provider_id"), dataset_assignment.getString("dataset_id"));
        BoundStatement boundStatement = bucketsForSpecificObjectIdStatement.bind(objectId);
        ResultSet rs = session.execute(boundStatement);
        Iterator<Row> ri = rs.iterator();

        while (hasNextRow(ri)) {
            UUID bucketId = ri.next().getUUID("bucket_id");
            BoundStatement countBoundStatement = countOfRowsStatement.bind(dataset_assignment.getString("provider_id"),
                    dataset_assignment.getString("dataset_id"),
                    bucketId,
                    dataset_assignment.getString("revision_provider_id"),
                    dataset_assignment.getString("revision_name"),
                    dataset_assignment.getDate("revision_timestamp"),
                    dataset_assignment.getString("representation_id"),
                    dataset_assignment.getString("cloud_id"));
            ResultSet resultSet = session.execute(countBoundStatement);
            long count = resultSet.one().getLong(0);
            if (count > 0)
                return count;
        }

        return 0;

    }

    private String createProviderDataSetId(String providerId, String dataSetId) {
        return providerId + CDSID_SEPARATOR + dataSetId;
    }
}


