package migrations.service.mcs.V18;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.*;
import eu.europeana.cloud.common.utils.Bucket;
import eu.europeana.cloud.service.commons.utils.BucketsHandler;

import java.util.Iterator;
import java.util.UUID;

import static migrations.common.TableCopier.hasNextRow;

/**
 * Created by Tarek on 4/26/19
 */
public class V18_1__copy_data_from_replica implements JavaMigration {

    private PreparedStatement selectStatement;
    private PreparedStatement insertStatement;

    private PreparedStatement bucketsForSpecificObjectIdStatement;
    private PreparedStatement countOfRowsStatement;


    private static final int BUCKET_SIZE = 200000;
    private static final String CDSID_SEPARATOR = "\n";
    private static final String BUCKET_TABLE_NAME = "latest_dataset_representation_revision_buckets";

    private void initStatements(Session session) {

        selectStatement = session.prepare("SELECT * FROM latest_provider_dataset_rep_rev_replica");

        insertStatement = session.prepare("INSERT INTO "
                + "latest_dataset_representation_revision_v1 (provider_id, dataset_id, bucket_id, cloud_id, representation_id, revision_timestamp, revision_name, revision_provider, version_id, acceptance, published, mark_deleted) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?);");


        bucketsForSpecificObjectIdStatement = session.prepare("Select bucket_id from latest_dataset_representation_revision_buckets where object_id=?");
        countOfRowsStatement = session.prepare("Select count(*) from latest_dataset_representation_revision_v1 where provider_id=? and dataset_id=? and bucket_id=? and representation_id=? and revision_name=? and revision_provider=? and mark_deleted=? and cloud_id=? ;");


        selectStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);
        insertStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);
        bucketsForSpecificObjectIdStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);
        countOfRowsStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);
    }

    @Override
    public void migrate(Session session) {
        initStatements(session);

        BucketsHandler bucketsHandler = new BucketsHandler(session);
        long counter = 0;

        BoundStatement boundStatement = selectStatement.bind();
        boundStatement.setFetchSize(1000);
        ResultSet rs = session.execute(boundStatement);

        Iterator<Row> ri = rs.iterator();

        while (hasNextRow(ri)) {
            Row latestProviderDatasetReplica = ri.next();
            if (getRowCount(session, latestProviderDatasetReplica) == 0) {

                Bucket bucket = bucketsHandler.getCurrentBucket(
                        BUCKET_TABLE_NAME,
                        createProviderDataSetId(latestProviderDatasetReplica.getString("provider_id"), latestProviderDatasetReplica.getString("dataset_id")));

                if (bucket == null || bucket.getRowsCount() >= BUCKET_SIZE) {
                    bucket = new Bucket(
                            createProviderDataSetId(latestProviderDatasetReplica.getString("provider_id"), latestProviderDatasetReplica.getString("dataset_id")),
                            new com.eaio.uuid.UUID().toString(),
                            0);
                }
                bucketsHandler.increaseBucketCount(BUCKET_TABLE_NAME, bucket);
                //
                BoundStatement insert = insertStatement.bind(
                        latestProviderDatasetReplica.getString("provider_id"),
                        latestProviderDatasetReplica.getString("dataset_id"),
                        UUID.fromString(bucket.getBucketId()),
                        latestProviderDatasetReplica.getString("cloud_id"),
                        latestProviderDatasetReplica.getString("representation_id"),
                        latestProviderDatasetReplica.getDate("revision_timestamp"),
                        latestProviderDatasetReplica.getString("revision_name"),
                        latestProviderDatasetReplica.getString("revision_provider"),
                        latestProviderDatasetReplica.getUUID("version_id"),
                        latestProviderDatasetReplica.getBool("acceptance"),
                        latestProviderDatasetReplica.getBool("published"),
                        latestProviderDatasetReplica.getBool("mark_deleted")
                );
                session.execute(insert);
                //
                if (++counter % 10000 == 0) {
                    System.out.print("\rCopy table progress: " + counter);
                }
            }
        }
    }


    private long getRowCount(Session session, Row latestProviderDatasetReplica) {

        String objectId = createProviderDataSetId(latestProviderDatasetReplica.getString("provider_id"), latestProviderDatasetReplica.getString("dataset_id"));
        BoundStatement boundStatement = bucketsForSpecificObjectIdStatement.bind(objectId);
        ResultSet rs = session.execute(boundStatement);
        Iterator<Row> ri = rs.iterator();

        while (hasNextRow(ri)) {
            UUID bucketId = ri.next().getUUID("bucket_id");
            BoundStatement countBoundStatement = countOfRowsStatement.bind(latestProviderDatasetReplica.getString("provider_id"),
                    latestProviderDatasetReplica.getString("dataset_id"),
                    bucketId,
                    latestProviderDatasetReplica.getString("representation_id"),
                    latestProviderDatasetReplica.getString("revision_name"),
                    latestProviderDatasetReplica.getString("revision_provider"),
                    latestProviderDatasetReplica.getBool("mark_deleted"),
                    latestProviderDatasetReplica.getString("cloud_id")

            );
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
