package eu.europeana.cloud.service.commons.utils;

import com.datastax.driver.core.*;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.utils.Bucket;
import eu.europeana.cloud.service.commons.cassandra.CassandraTestBase;
import org.junit.*;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/context.xml"})
public class BucketsHandlerTest extends CassandraTestBase {

    @Autowired
    private BucketsHandler bucketsHandler;

    @Autowired
    private CassandraConnectionProvider dbService;

    private static final String BUCKETS_TABLE_NAME = "provider_record_id_buckets";

    @Test
    public void currentBucketShouldBeNull() {
        Bucket bucket = bucketsHandler.getCurrentBucket(BUCKETS_TABLE_NAME, "sampleObject");
        Assert.assertNull(bucket);
    }

    @Test
    public void shouldCreateNewBucket() {
        Bucket bucket = new Bucket("sampleObjectId", new com.eaio.uuid.UUID().toString(), 0);
        bucketsHandler.increaseBucketCount(BUCKETS_TABLE_NAME, bucket);
        //
        assertResults(bucket, 1);
    }

    @Test
    public void shouldUpdateCounterForExistingBucket() {
        Bucket bucket = new Bucket("sampleObjectId", new com.eaio.uuid.UUID().toString(), 0);
        bucketsHandler.increaseBucketCount(BUCKETS_TABLE_NAME, bucket);
        bucketsHandler.increaseBucketCount(BUCKETS_TABLE_NAME, bucket);

        assertResults(bucket, 2);
    }

    @Test
    public void shouldDecreaseCounterForExistingBucket() {
        Bucket bucket = new Bucket("sampleObjectId", new com.eaio.uuid.UUID().toString(), 0);
        bucketsHandler.increaseBucketCount(BUCKETS_TABLE_NAME, bucket);
        bucketsHandler.increaseBucketCount(BUCKETS_TABLE_NAME, bucket);
        bucketsHandler.increaseBucketCount(BUCKETS_TABLE_NAME, bucket);
        bucketsHandler.decreaseBucketCount(BUCKETS_TABLE_NAME, bucket);

        assertResults(bucket, 2);
    }

    @Test
    public void shouldListAllBucketsForGivenObjectId() {
        //for
        Bucket firstBucket = new Bucket("sampleObjectId", new com.eaio.uuid.UUID().toString(), 0);
        Bucket secondBucket = new Bucket("sampleObjectId", new com.eaio.uuid.UUID().toString(), 0);
        bucketsHandler.increaseBucketCount(BUCKETS_TABLE_NAME, firstBucket);
        bucketsHandler.increaseBucketCount(BUCKETS_TABLE_NAME, secondBucket);
        //when
        List<Bucket> buckets = bucketsHandler.getAllBuckets(BUCKETS_TABLE_NAME, firstBucket.getObjectId());
        //then
        Assert.assertTrue(buckets.size() == 2);
        for (Bucket bucket : buckets) {
            Assert.assertTrue(bucket.getRowsCount() == 1);
        }
    }

    @Test
    public void shouldNextBucketBeReturned() {
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
        Assert.assertTrue(bucket.getRowsCount() == 1);
        Assert.assertTrue(bucket.getBucketId().equals(secondBucket.getBucketId()));
    }

    @Test
    public void shouldEmptyBucketBeReturned() {
        //for
        Bucket firstBucket = new Bucket("sampleObjectId", new com.eaio.uuid.UUID().toString(), 0);
        Bucket secondBucket = new Bucket("sampleObjectId", new com.eaio.uuid.UUID().toString(), 0);
        bucketsHandler.increaseBucketCount(BUCKETS_TABLE_NAME, firstBucket);
        bucketsHandler.increaseBucketCount(BUCKETS_TABLE_NAME, secondBucket);
        //when
        Bucket bucket = bucketsHandler.getNextBucket(BUCKETS_TABLE_NAME, secondBucket.getObjectId(), secondBucket);
        //then
        Assert.assertNull(bucket);
    }

    @Test
    public void shouldReturnFirstBucket() {
        //for
        Bucket firstBucket = new Bucket("sampleObjectId_1", new com.eaio.uuid.UUID().toString(), 1);
        Bucket secondBucket = new Bucket("sampleObjectId_2", new com.eaio.uuid.UUID().toString(), 1);
        bucketsHandler.increaseBucketCount(BUCKETS_TABLE_NAME, firstBucket);
        bucketsHandler.increaseBucketCount(BUCKETS_TABLE_NAME, secondBucket);
        //when
        Bucket bucket_1 = bucketsHandler.getNextBucket(BUCKETS_TABLE_NAME, firstBucket.getObjectId());
        Bucket bucket_2 = bucketsHandler.getNextBucket(BUCKETS_TABLE_NAME, secondBucket.getObjectId());
        //then
        Assert.assertTrue(bucket_1.getObjectId().equals(firstBucket.getObjectId()));
        Assert.assertTrue(bucket_1.getRowsCount() == firstBucket.getRowsCount());
        Assert.assertTrue(bucket_1.getBucketId().equals(firstBucket.getBucketId()));
        //
        Assert.assertTrue(bucket_2.getObjectId().equals(secondBucket.getObjectId()));
        Assert.assertTrue(bucket_2.getRowsCount() == secondBucket.getRowsCount());
        Assert.assertTrue(bucket_2.getBucketId().equals(secondBucket.getBucketId()));
    }

    @Test
    public void shouldRemoveBucket() {
        //for
        Bucket firstBucket = new Bucket("sampleObjectId_1", new com.eaio.uuid.UUID().toString(), 1);
        Bucket secondBucket = new Bucket("sampleObjectId_2", new com.eaio.uuid.UUID().toString(), 1);
        bucketsHandler.increaseBucketCount(BUCKETS_TABLE_NAME, firstBucket);
        bucketsHandler.increaseBucketCount(BUCKETS_TABLE_NAME, secondBucket);
        //when
        bucketsHandler.removeBucket(BUCKETS_TABLE_NAME, firstBucket);
        //then
        ResultSet rs = dbService.getSession().execute("SELECT * FROM " + BUCKETS_TABLE_NAME + " WHERE object_id='" + firstBucket.getObjectId() + "' AND bucket_id=" + java.util.UUID.fromString(firstBucket.getBucketId()));
        List<Row> rows = rs.all();
        Assert.assertTrue(rows.size() == 0);
        //
        rs = dbService.getSession().execute("SELECT * FROM " + BUCKETS_TABLE_NAME + " WHERE object_id='" + secondBucket.getObjectId() + "' AND bucket_id=" + java.util.UUID.fromString(secondBucket.getBucketId()));
        rows = rs.all();
        Assert.assertTrue(rows.size() == 1);
        Assert.assertTrue(rows.get(0).getString(BucketsHandler.OBJECT_ID_COLUMN_NAME).equals(secondBucket.getObjectId()));
        Assert.assertTrue(rows.get(0).getUUID(BucketsHandler.BUCKET_ID_COLUMN_NAME).toString().equals(secondBucket.getBucketId()));
        Assert.assertTrue(rows.get(0).getLong(BucketsHandler.ROWS_COUNT_COLUMN_NAME) == secondBucket.getRowsCount());
    }

    private void assertResults(Bucket bucket, int rowsCount) {
        ResultSet rs = dbService.getSession().execute("SELECT * FROM " + BUCKETS_TABLE_NAME + " WHERE object_id='" + bucket.getObjectId() + "' AND bucket_id=" + java.util.UUID.fromString(bucket.getBucketId()));
        List<Row> rows = rs.all();
        Assert.assertTrue(rows.size() == 1);
        Assert.assertTrue(rows.get(0).getString("object_id").equals(bucket.getObjectId()));
        Assert.assertTrue(rows.get(0).getUUID("bucket_id").equals(java.util.UUID.fromString(bucket.getBucketId())));
        Assert.assertTrue(rows.get(0).getLong("rows_count") == rowsCount);
    }
}
