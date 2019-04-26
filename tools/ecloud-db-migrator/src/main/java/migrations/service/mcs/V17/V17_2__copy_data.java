package migrations.service.mcs.V17;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.*;
import eu.europeana.cloud.common.utils.Bucket;
import eu.europeana.cloud.service.commons.utils.BucketsHandler;

import java.util.Iterator;
import java.util.UUID;

import static migrations.common.TableCopier.hasNextRow;

/**
 * Created by pwozniak on 4/24/19
 */
public class V17_2__copy_data implements JavaMigration {

    private PreparedStatement selectStatement;
    private PreparedStatement insertStatement;

    private static final int BUCKET_SIZE = 200000;
    private static final String CDSID_SEPARATOR = "\n";
    private static final String BUCKET_TABLE_NAME = "latest_dataset_representation_revision_buckets";

    private void initStatements(Session session) {

        selectStatement = session.prepare("SELECT * FROM latest_provider_dataset_representation_revision");

        insertStatement = session.prepare("INSERT INTO "
                + "latest_dataset_representation_revision_v1 (provider_id, dataset_id, bucket_id, cloud_id, representation_id, revision_timestamp, revision_name, revision_provider, version_id, acceptance, published, mark_deleted) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?);");

        selectStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);
        insertStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);
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
            Row row = ri.next();

            Bucket bucket = bucketsHandler.getCurrentBucket(
                    BUCKET_TABLE_NAME,
                    createProviderDataSetId(row.getString("provider_id"), row.getString("dataset_id")));

            if (bucket == null || bucket.getRowsCount() >= BUCKET_SIZE) {
                bucket = new Bucket(
                        createProviderDataSetId(row.getString("provider_id"), row.getString("dataset_id")),
                        new com.eaio.uuid.UUID().toString(),
                        0);
            }
            bucketsHandler.increaseBucketCount(BUCKET_TABLE_NAME, bucket);
            //
            BoundStatement insert = insertStatement.bind(
                    row.getString("provider_id"),
                    row.getString("dataset_id"),
                    UUID.fromString(bucket.getBucketId()),
                    row.getString("cloud_id"),
                    row.getString("representation_id"),
                    row.getDate("revision_timestamp"),
                    row.getString("revision_name"),
                    row.getString("revision_provider"),
                    row.getUUID("version_id"),
                    row.getBool("acceptance"),
                    row.getBool("published"),
                    row.getBool("mark_deleted")
            );
            session.execute(insert);
            //
            if (++counter % 10000 == 0) {
                System.out.print("\rCopy table progress: " + counter);
            }
        }
    }

    private String createProviderDataSetId(String providerId, String dataSetId) {
        return providerId + CDSID_SEPARATOR + dataSetId;
    }
}
