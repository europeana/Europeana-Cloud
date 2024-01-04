package eu.europeana.cloud.service.commons.utils;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import eu.europeana.cloud.common.annotation.Retryable;
import eu.europeana.cloud.common.utils.Bucket;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * This class is responsible for handling all operations related to data bucketing for any table (that requires bucketing). <br/>
 * Buckets table has to have proper structure.
 */
@Retryable
public class BucketsHandler {

  public static final String OBJECT_ID_COLUMN_NAME = "object_id";
  public static final String BUCKET_ID_COLUMN_NAME = "bucket_id";
  public static final String ROWS_COUNT_COLUMN_NAME = "rows_count";
  //
  private Session session;

  /**
   * Creates new {@link BucketsHandler} instance
   *
   * @param session cassandra connection session need for handler
   */
  public BucketsHandler(Session session) {
    this.session = session;
  }

  /**
   * Gets current (the latest) bucket for given table and object (bucket identifier)
   *
   * @param bucketsTableName table name
   * @param objectId object identifier
   *
   * @return the latest bucket for given parameters
   */
  public Bucket getCurrentBucket(String bucketsTableName, String objectId) {
    String query = "SELECT object_id, bucket_id, rows_count FROM " + bucketsTableName
        + " WHERE object_id = '" + objectId + "';";
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

  /**
   * Increases by one number of elements in given bucket
   *
   * @param bucketsTableName table name
   * @param bucket bucket identifier
   */
  public void increaseBucketCount(String bucketsTableName, Bucket bucket) {
    String query = "UPDATE " + bucketsTableName + " SET rows_count = rows_count + 1 WHERE object_id = '"
        + bucket.getObjectId() + "' AND bucket_id = " + UUID.fromString(bucket.getBucketId()) + ";";
    session.execute(query);
  }

  /**
   * Decreases by one number of elements in given bucket
   *
   * @param bucketsTableName table name
   * @param bucket bucket identifier
   */
  public void decreaseBucketCount(String bucketsTableName, Bucket bucket) {
    String query = "UPDATE " + bucketsTableName + " SET rows_count = rows_count - 1 WHERE object_id = '"
        + bucket.getObjectId() + "' AND bucket_id = " + UUID.fromString(bucket.getBucketId()) + ";";
    session.execute(query);

    Bucket actual = getBucket(bucketsTableName, bucket);
    if (actual != null && actual.getRowsCount() == 0) {
      removeBucket(bucketsTableName, bucket);
    }
  }

  /**
   * List all the buckets for given bucket identifier
   *
   * @param bucketsTableName table name
   * @param objectId object identifier
   *
   * @return list of all buckets related with given bucket identifier
   */
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


  /**
   * Gets the bucket from the database based on the provided parameters
   *
   * @param bucketsTableName table name
   * @param bucket bucket instance that will be used
   *
   * @return found bucket
   */
  public Bucket getBucket(String bucketsTableName, Bucket bucket) {
    String query = "SELECT * FROM " + bucketsTableName + " where object_id = '" + bucket.getObjectId()
        + "' AND bucket_id = " + UUID.fromString(bucket.getBucketId()) + " LIMIT 1;";
    return getBucket(query);
  }

  /**
   * Gets first bucket from the database based on the provided parameters
   *
   * @param bucketsTableName table name
   * @param objectId object identifier
   *
   * @return found bucket
   */
  public Bucket getFirstBucket(String bucketsTableName, String objectId) {
    String query = "SELECT * FROM " + bucketsTableName + " where object_id = '" + objectId + "' LIMIT 1;";
    return getBucket(query);
  }

  /**
   * Gets bucket from the database that is after the bucket specified in the parameters
   * @param bucketsTableName table name
   * @param objectId object identifier
   * @param bucket bucket used as reference
   *
   * @return found bucket. Will return null if there is no next bucket
   */
  public Bucket getNextBucket(String bucketsTableName, String objectId, Bucket bucket) {
    String query = "SELECT * FROM " + bucketsTableName + " where object_id = '" + objectId
        + "' AND bucket_id > " + UUID.fromString(bucket.getBucketId()) + " LIMIT 1;";
    return getBucket(query);
  }

  /**
   * Gets bucket from the database that is before the bucket specified in the parameters
   * @param bucketsTableName table name
   * @param objectId object identifier
   *
   * @return found bucket. Will return null if there is no next bucket
   */
  public Bucket getPreviousBucket(String bucketsTableName, String objectId) {
    String query = "SELECT * FROM " + bucketsTableName + " where object_id = '" + objectId
        + "' ORDER BY bucket_id DESC LIMIT 1;";
    return getBucket(query);
  }

  /**
   * Gets bucket from the database that is before the bucket specified in the parameters
   * @param bucketsTableName table name
   * @param objectId object identifier
   * @param bucket bucket used as reference
   *
   * @return found bucket. Will return null if there is no next bucket
   */
  public Bucket getPreviousBucket(String bucketsTableName, String objectId, Bucket bucket) {
    String query = "SELECT * FROM " + bucketsTableName + " where object_id = '" + objectId
        + "' AND bucket_id < " + UUID.fromString(bucket.getBucketId()) + " ORDER BY bucket_id DESC LIMIT 1;";
    return getBucket(query);
  }

  /**
   * Removes bucket from database
   *
   * @param bucketsTableName table name
   * @param bucket bucket to be removed
   *
   */
  public void removeBucket(String bucketsTableName, Bucket bucket) {
    String query = "DELETE FROM " + bucketsTableName + " WHERE object_id = '" + bucket.getObjectId()
        + "' AND bucket_id = " + UUID.fromString(bucket.getBucketId()) + ";";
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
