package eu.europeana.cloud.service.commons.utils;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import eu.europeana.cloud.common.utils.Bucket;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * This class is responsible for handling all operations related to data bucketing for any table (that requires bucketing).
 * <br/>
 * Buckets table has to have proper structure.
 */
public class BucketsHandler {

    public static final String OBJECT_ID_COLUMN_NAME = "object_id";
    public static final String BUCKET_ID_COLUMN_NAME = "bucket_id";
    public static final String ROWS_COUNT_COLUMN_NAME = "rows_count";
    //
    private Session session;

    public BucketsHandler(Session session) {
        this.session = session;
    }

    public Bucket getCurrentBucket(String bucketsTableName, String objectId) {
        String query = "SELECT object_id, bucket_id, rows_count FROM " + bucketsTableName + " WHERE object_id = '" + objectId + "';";
        ResultSet rs = session.execute(query);

        List<Row> rows = rs.all();
        Row row = rows.isEmpty() ? null : rows.get(rows.size() - 1);
        if (row != null) {
            return new Bucket(
                    row.getString(OBJECT_ID_COLUMN_NAME),
                    row.getUUID(BUCKET_ID_COLUMN_NAME).toString(),
                    row.getLong(ROWS_COUNT_COLUMN_NAME));
        }
        return null;
    }

    public void increaseBucketCount(String bucketsTableName, Bucket bucket) {
        String query = "UPDATE " + bucketsTableName + " SET rows_count = rows_count + 1 WHERE object_id = '" + bucket.getObjectId() + "' AND bucket_id = " + UUID.fromString(bucket.getBucketId()) + ";";
        session.execute(query);
    }

    public void decreaseBucketCount(String bucketsTableName, Bucket bucket) {
        String query = "UPDATE " + bucketsTableName + " SET rows_count = rows_count - 1 WHERE object_id = '" + bucket.getObjectId() + "' AND bucket_id = " + UUID.fromString(bucket.getBucketId()) + ";";
        session.execute(query);

        Bucket actual = getBucket(bucketsTableName, bucket);
        if (actual != null && actual.getRowsCount() == 0) {
            removeBucket(bucketsTableName, bucket);
        }
    }

    public List<Bucket> getAllBuckets(String bucketsTableName, String objectId) {
        String query = "SELECT * FROM " + bucketsTableName + " WHERE object_id = '" + objectId + "';";
        ResultSet rs = session.execute(query);


        List<Row> rows = rs.all();
        List<Bucket> resultBuckets = new ArrayList<>(rows.size());
        for (Row row : rows) {
            Bucket bucket = new Bucket(
                    row.getString(OBJECT_ID_COLUMN_NAME),
                    row.getUUID(BUCKET_ID_COLUMN_NAME).toString(),
                    row.getLong(ROWS_COUNT_COLUMN_NAME));
            resultBuckets.add(bucket);
        }
        return resultBuckets;
    }


    public Bucket getBucket(String bucketsTableName, Bucket bucket) {
        String query = "SELECT * FROM " + bucketsTableName + " where object_id = '" + bucket.getObjectId() + "' AND bucket_id = " + UUID.fromString(bucket.getBucketId()) + " LIMIT 1;";
        return getBucket(query);
    }

    public Bucket getNextBucket(String bucketsTableName, String objectId) {
        String query = "SELECT * FROM " + bucketsTableName + " where object_id = '" + objectId + "' LIMIT 1;";
        return getBucket(query);
    }

    public Bucket getNextBucket(String bucketsTableName, String objectId, Bucket bucket) {
        String query = "SELECT * FROM " + bucketsTableName + " where object_id = '" + objectId + "' AND bucket_id > " + UUID.fromString(bucket.getBucketId()) + " LIMIT 1;";
        return getBucket(query);
    }

    public Bucket getPreviousBucket(String bucketsTableName, String objectId) {
        String query = "SELECT * FROM " + bucketsTableName + " where object_id = '" + objectId + "' ORDER BY bucket_id DESC LIMIT 1;";
        return getBucket(query);
    }

    public Bucket getPreviousBucket(String bucketsTableName, String objectId, Bucket bucket) {
        String query = "SELECT * FROM " + bucketsTableName + " where object_id = '" + objectId + "' AND bucket_id < " + UUID.fromString(bucket.getBucketId()) + " ORDER BY bucket_id DESC LIMIT 1;";
        return getBucket(query);
    }

    public void removeBucket(String bucketsTableName, Bucket bucket) {
        String query = "DELETE FROM " + bucketsTableName + " WHERE object_id = '" + bucket.getObjectId() + "' AND bucket_id = " + UUID.fromString(bucket.getBucketId()) + ";";
        session.execute(query);
    }

    private Bucket getBucket(String query) {
        ResultSet rs = session.execute(query);
        Row row = rs.one();
        Bucket resultBucket = null;
        if (row != null) {
            resultBucket = new Bucket(
                    row.getString(OBJECT_ID_COLUMN_NAME),
                    row.getUUID(BUCKET_ID_COLUMN_NAME).toString(),
                    row.getLong(ROWS_COUNT_COLUMN_NAME));
        }
        return resultBucket;
    }
}