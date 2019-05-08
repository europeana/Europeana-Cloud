package migrations.service.mcs.V17.jobs;

import com.datastax.driver.core.*;
import eu.europeana.cloud.common.utils.Bucket;
import eu.europeana.cloud.service.commons.utils.BucketsHandler;

import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.Callable;

import static migrations.common.TableCopier.hasNextRow;

/**
 * Created by Tarek on 5/8/2019.
 */
public class DataCopier implements Callable<String> {
    private static final String PROVIDER_ID = "provider_id";
    private static final String DATASET_ID = "dataset_id";
    private static final int DEFAULT_RETRIES = 3;
    private static final int SLEEP_TIME = 5000;

    private Session session;
    private Row distinctPartitionKeysRow;

    private PreparedStatement selectStatement;
    private PreparedStatement insertStatement;

    private static final int BUCKET_SIZE = 200000;
    private static final String CDSID_SEPARATOR = "\n";
    private static final String BUCKET_TABLE_NAME = "latest_dataset_representation_revision_buckets";

    public DataCopier(Session session, Row distinctPartitionKeysRow) {
        this.session = session;
        this.distinctPartitionKeysRow = distinctPartitionKeysRow;
        initStatements();
    }

    private void initStatements() {
        selectStatement = session.prepare("SELECT * FROM latest_provider_dataset_representation_revision where provider_id=? and dataset_id=?; ");

        insertStatement = session.prepare("INSERT INTO "
                + "latest_dataset_representation_revision_v1 (provider_id, dataset_id, bucket_id, cloud_id, representation_id, revision_timestamp, revision_name, revision_provider, version_id, acceptance, published, mark_deleted) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?,?);");

        selectStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);
        insertStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);
    }

    @Override
    public String call() throws Exception {
        BucketsHandler bucketsHandler = new BucketsHandler(session);
        long counter = 0;
        final String providerId = distinctPartitionKeysRow.getString(PROVIDER_ID);
        String dataSetId = distinctPartitionKeysRow.getString(DATASET_ID);
        BoundStatement boundStatement = selectStatement.bind(providerId, dataSetId);
        boundStatement.setFetchSize(1000);
        ResultSet rs = session.execute(boundStatement);
        Iterator<Row> ri = rs.iterator();

        while (hasNextRow(ri)) {
            Row row = ri.next();

            Bucket bucket = bucketsHandler.getCurrentBucket(
                    BUCKET_TABLE_NAME,
                    createProviderDataSetId(providerId, dataSetId));

            if (bucket == null || bucket.getRowsCount() >= BUCKET_SIZE) {
                bucket = new Bucket(
                        createProviderDataSetId(providerId, dataSetId),
                        new com.eaio.uuid.UUID().toString(),
                        0);
            }
            bucketsHandler.increaseBucketCount(BUCKET_TABLE_NAME, bucket);
            //
            insertIntoNewTable(row, bucket.getBucketId());
            //
            if (++counter % 10000 == 0) {
                System.out.print("\rCopy table for providerId: " + providerId + " and datasetId " + dataSetId + "the current progress is:" + counter);
            }
        }
        return "................... The information for providerId: " + providerId + " and datasetId " + dataSetId + " is inserted correctly. The total number of inserted rows is:" + counter;
    }

    private void insertIntoNewTable(Row row, String bucketId) {
        BoundStatement insert = insertStatement.bind(
                row.getString(PROVIDER_ID),
                row.getString(DATASET_ID),
                UUID.fromString(bucketId),
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


        int retries = DEFAULT_RETRIES;

        while (true) {
            try {
                session.execute(insert);
                break;
            } catch (Exception e) {
                if (retries-- > 0) {
                    System.out.println("Warning while inserting to latest_dataset_representation_revision_v1. Retries left:" + retries);
                    try {
                        Thread.sleep(SLEEP_TIME);
                    } catch (InterruptedException e1) {
                        Thread.currentThread().interrupt();
                        System.err.println(e1.getMessage());
                    }
                } else {
                    System.err.println("Error while inserting to latest_dataset_representation_revision_v1. " + insert.preparedStatement().getQueryString());
                    throw e;
                }
            }
        }
    }

    private String createProviderDataSetId(String providerId, String dataSetId) {
        return providerId + CDSID_SEPARATOR + dataSetId;
    }
}
