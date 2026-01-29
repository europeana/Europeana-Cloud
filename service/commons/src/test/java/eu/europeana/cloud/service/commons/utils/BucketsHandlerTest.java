package eu.europeana.cloud.service.commons.utils;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.utils.Bucket;
import eu.europeana.cloud.service.commons.cassandra.CassandraTestBase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(locations = {"classpath:/context.xml"})
class BucketsHandlerTest extends CassandraTestBase {

  @Autowired
  private BucketsHandler bucketsHandler;

  @Autowired
  private CassandraConnectionProvider dbService;

  private static final String BUCKETS_TABLE_NAME = "data_set_assignments_by_data_set_buckets";

  @Test
  void currentBucketShouldBeNull() {
    Bucket bucket = bucketsHandler.getCurrentBucket(BUCKETS_TABLE_NAME, "sampleObject");
    assertNull(bucket);
  }

  @Test
  void shouldCreateNewBucket() {
    Bucket bucket = new Bucket("sampleObjectId", new com.eaio.uuid.UUID().toString(), 0);
    bucketsHandler.increaseBucketCount(BUCKETS_TABLE_NAME, bucket);
    //
    assertResults(bucket, 1);
  }

  @Test
  void shouldUpdateCounterForExistingBucket() {
    Bucket bucket = new Bucket("sampleObjectId", new com.eaio.uuid.UUID().toString(), 0);
    bucketsHandler.increaseBucketCount(BUCKETS_TABLE_NAME, bucket);
    bucketsHandler.increaseBucketCount(BUCKETS_TABLE_NAME, bucket);

    assertResults(bucket, 2);
  }

  @Test
  void shouldDecreaseCounterForExistingBucket() {
    Bucket bucket = new Bucket("sampleObjectId", new com.eaio.uuid.UUID().toString(), 0);
    bucketsHandler.increaseBucketCount(BUCKETS_TABLE_NAME, bucket);
    bucketsHandler.increaseBucketCount(BUCKETS_TABLE_NAME, bucket);
    bucketsHandler.increaseBucketCount(BUCKETS_TABLE_NAME, bucket);
    bucketsHandler.decreaseBucketCount(BUCKETS_TABLE_NAME, bucket);

    assertResults(bucket, 2);
  }

  @Test
  void shouldListAllBucketsForGivenObjectId() {
    //for
    Bucket firstBucket = new Bucket("sampleObjectId", new com.eaio.uuid.UUID().toString(), 0);
    Bucket secondBucket = new Bucket("sampleObjectId", new com.eaio.uuid.UUID().toString(), 0);
    bucketsHandler.increaseBucketCount(BUCKETS_TABLE_NAME, firstBucket);
    bucketsHandler.increaseBucketCount(BUCKETS_TABLE_NAME, secondBucket);
    //when
    List<Bucket> buckets = bucketsHandler.getAllBuckets(BUCKETS_TABLE_NAME, firstBucket.getObjectId());
    //then
    assertEquals(2, buckets.size());
    for (Bucket bucket : buckets) {
      assertEquals(1, bucket.getRowsCount());
    }
  }

  @Test
  void shouldNextBucketBeReturned() {
    //for
    Bucket firstBucket = new Bucket("sampleObjectId", new com.eaio.uuid.UUID().toString(), 0);
    Bucket secondBucket = new Bucket("sampleObjectId", new com.eaio.uuid.UUID().toString(), 0);
    Bucket thirdBucket = new Bucket("sampleObjectId", new com.eaio.uuid.UUID().toString(), 0);
    bucketsHandler.increaseBucketCount(BUCKETS_TABLE_NAME, firstBucket);
    bucketsHandler.increaseBucketCount(BUCKETS_TABLE_NAME, secondBucket);
    bucketsHandler.increaseBucketCount(BUCKETS_TABLE_NAME, thirdBucket);
    //when
    Bucket bucket = bucketsHandler.getNextBucket(BUCKETS_TABLE_NAME, firstBucket.getObjectId(), firstBucket);
    //then
    assertEquals(1, bucket.getRowsCount());
    assertEquals(secondBucket.getBucketId(), bucket.getBucketId());
  }

  @Test
  void shouldEmptyBucketBeReturned() {
    //for
    Bucket firstBucket = new Bucket("sampleObjectId", new com.eaio.uuid.UUID().toString(), 0);
    Bucket secondBucket = new Bucket("sampleObjectId", new com.eaio.uuid.UUID().toString(), 0);
    bucketsHandler.increaseBucketCount(BUCKETS_TABLE_NAME, firstBucket);
    bucketsHandler.increaseBucketCount(BUCKETS_TABLE_NAME, secondBucket);
    //when
    Bucket bucket = bucketsHandler.getNextBucket(BUCKETS_TABLE_NAME, secondBucket.getObjectId(), secondBucket);
    //then
    assertNull(bucket);
  }

  @Test
  void shouldReturnFirstBucket() {
    //for
    Bucket bucketForObject1 = new Bucket("sampleObjectId_1", new com.eaio.uuid.UUID().toString(), 1);
    Bucket bucketForObject2 = new Bucket("sampleObjectId_2", new com.eaio.uuid.UUID().toString(), 1);
    bucketsHandler.increaseBucketCount(BUCKETS_TABLE_NAME, bucketForObject1);
    bucketsHandler.increaseBucketCount(BUCKETS_TABLE_NAME, bucketForObject2);
    //when
    Bucket bucket1 = bucketsHandler.getFirstBucket(BUCKETS_TABLE_NAME, bucketForObject1.getObjectId());
    Bucket bucket2 = bucketsHandler.getFirstBucket(BUCKETS_TABLE_NAME, bucketForObject2.getObjectId());
    //then
    assertEquals(bucketForObject1.getObjectId(), bucket1.getObjectId());
    assertEquals(bucketForObject1.getRowsCount(), bucket1.getRowsCount());
    assertEquals(bucketForObject1.getBucketId(), bucket1.getBucketId());
    //
    assertEquals(bucketForObject2.getObjectId(), bucket2.getObjectId());
    assertEquals(bucketForObject2.getRowsCount(), bucket2.getRowsCount());
    assertEquals(bucketForObject2.getBucketId(), bucket2.getBucketId());
  }

  @Test
  void shouldRemoveBucket() {
    //for
    Bucket firstBucket = new Bucket("sampleObjectId_1", new com.eaio.uuid.UUID().toString(), 1);
    Bucket secondBucket = new Bucket("sampleObjectId_2", new com.eaio.uuid.UUID().toString(), 1);
    bucketsHandler.increaseBucketCount(BUCKETS_TABLE_NAME, firstBucket);
    bucketsHandler.increaseBucketCount(BUCKETS_TABLE_NAME, secondBucket);
    //when
    bucketsHandler.removeBucket(BUCKETS_TABLE_NAME, firstBucket);
    //then
    ResultSet rs = dbService.getSession().execute(
            "SELECT * FROM " + BUCKETS_TABLE_NAME + " WHERE object_id='" + firstBucket.getObjectId() + "' AND bucket_id="
                    + java.util.UUID.fromString(firstBucket.getBucketId()));
    List<Row> rows = rs.all();
    assertEquals(0, rows.size());
    //
    rs = dbService.getSession().execute(
            "SELECT * FROM " + BUCKETS_TABLE_NAME + " WHERE object_id='" + secondBucket.getObjectId() + "' AND bucket_id="
                    + java.util.UUID.fromString(secondBucket.getBucketId()));
    rows = rs.all();
    assertEquals(1, rows.size());
    assertEquals(secondBucket.getObjectId(), rows.get(0).getString(BucketsHandler.OBJECT_ID_COLUMN_NAME));
    assertEquals(secondBucket.getBucketId(), rows.get(0).getUUID(BucketsHandler.BUCKET_ID_COLUMN_NAME).toString());
    assertEquals(secondBucket.getRowsCount(), rows.get(0).getLong(BucketsHandler.ROWS_COUNT_COLUMN_NAME));
  }

  private void assertResults(Bucket bucket, int rowsCount) {
    ResultSet rs = dbService.getSession().execute(
            "SELECT * FROM " + BUCKETS_TABLE_NAME + " WHERE object_id='" + bucket.getObjectId() + "' AND bucket_id="
                    + java.util.UUID.fromString(bucket.getBucketId()));
    List<Row> rows = rs.all();
    assertEquals(1, rows.size());
    assertEquals(bucket.getObjectId(), rows.get(0).getString("object_id"));
    assertEquals(java.util.UUID.fromString(bucket.getBucketId()), rows.get(0).getUUID("bucket_id"));
    assertEquals(rowsCount, rows.get(0).getLong("rows_count"));
  }
}
