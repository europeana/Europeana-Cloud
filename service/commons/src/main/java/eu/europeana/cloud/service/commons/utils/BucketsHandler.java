package eu.europeana.cloud.service.commons.utils;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import eu.europeana.cloud.common.utils.Bucket;

import java.util.ArrayList;
import java.util.List;

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
        String query = "UPDATE " + bucketsTableName + " SET rows_count = rows_count + 1 WHERE object_id = '" + bucket.getObjectId() + "' AND bucket_id = " + bucket.getBucketId() + ";";
        session.execute(query);
    }

    public void decreaseBucketCount(String bucketsTableName, Bucket bucket) {
        String query = "UPDATE " + bucketsTableName + " SET rows_count = rows_count - 1 WHERE object_id = '" + bucket.getObjectId() + "' AND bucket_id = " + bucket.getBucketId() + ";";
        session.execute(query);
    }

    public List<Bucket> getAllBuckets(String bucketsTableName, String objectId) {
        String query = "SELECT * FROM " + bucketsTableName + " WHERE object_id = '" + objectId + "';";
        ResultSet rs = session.execute(query);

        List<Bucket> resultBuckets = new ArrayList<>();

        List<Row> rows = rs.all();
        for (Row row : rows) {
            Bucket bucket = new Bucket(
                    row.getString(OBJECT_ID_COLUMN_NAME),
                    row.getUUID(BUCKET_ID_COLUMN_NAME).toString(),
                    row.getLong(ROWS_COUNT_COLUMN_NAME));
            resultBuckets.add(bucket);
        }
        return resultBuckets;
    }

    public Bucket getNextBucket(String bucketsTableName, String objectId) {
        String query = "SELECT * FROM " + bucketsTableName + " where object_id = '" + objectId + "' LIMIT 1;";
        return getNextBucket(query);
    }

    public Bucket getNextBucket(String bucketsTableName, String objectId, Bucket bucket) {
        String query = "SELECT * FROM " + bucketsTableName + " where object_id = '" + objectId + "' AND bucket_id > " + bucket.getBucketId() + " LIMIT 1;";
        return getNextBucket(query);
    }

    public void removeBucket(String bucketsTableName, Bucket bucket) {
        String query = "DELETE FROM " + bucketsTableName + " WHERE object_id = '" + bucket.getObjectId() + "' AND bucket_id = " + bucket.getBucketId() + ";";
        session.execute(query);
    }

    private Bucket getNextBucket(String query) {
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