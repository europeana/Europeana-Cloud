package migrator.validators;

import com.datastax.driver.core.*;
import migrator.EmbeddedCassandra;
import org.junit.Assert;

public class V10_validator {

  private Session session;

  public V10_validator(Session session) {
    this.session = session;
  }

  public void validate() {
    KeyspaceMetadata meta = session.getCluster().getMetadata().getKeyspace(EmbeddedCassandra.KEYSPACE);
    TableMetadata assignments_by_data_set_buckets = meta.getTable("data_set_assignments_by_data_set_buckets");
    TableMetadata assignments_by_representations = meta.getTable("data_set_assignments_by_representations");
    TableMetadata assignments_by_data_set = meta.getTable("data_set_assignments_by_data_set");
    TableMetadata assignments = meta.getTable("data_set_assignments");

    Assert.assertNotNull(assignments_by_data_set);
    Assert.assertNotNull(assignments_by_data_set_buckets);
    Assert.assertNull(assignments);
    //
    Assert.assertNotNull(assignments_by_data_set_buckets.getColumn("object_id"));
    Assert.assertNotNull(assignments_by_data_set_buckets.getColumn("bucket_id"));
    Assert.assertNotNull(assignments_by_data_set_buckets.getColumn("rows_count"));

    Assert.assertEquals(assignments_by_data_set_buckets.getColumn("object_id").getType(), DataType.varchar());
    Assert.assertEquals(assignments_by_data_set_buckets.getColumn("bucket_id").getType(), DataType.timeuuid());
    Assert.assertEquals(assignments_by_data_set_buckets.getColumn("rows_count").getType(), DataType.counter());
    //
    Assert.assertNotNull(assignments_by_data_set.getColumn("provider_dataset_id"));
    Assert.assertNotNull(assignments_by_data_set.getColumn("bucket_id"));
    Assert.assertNotNull(assignments_by_data_set.getColumn("cloud_id"));
    Assert.assertNotNull(assignments_by_data_set.getColumn("schema_id"));
    Assert.assertNotNull(assignments_by_data_set.getColumn("version_id"));
    Assert.assertNotNull(assignments_by_data_set.getColumn("creation_date"));

    Assert.assertEquals(assignments_by_data_set.getColumn("provider_dataset_id").getType(), DataType.varchar());
    Assert.assertEquals(assignments_by_data_set.getColumn("bucket_id").getType(), DataType.timeuuid());
    Assert.assertEquals(assignments_by_data_set.getColumn("cloud_id").getType(), DataType.varchar());
    Assert.assertEquals(assignments_by_data_set.getColumn("schema_id").getType(), DataType.varchar());
    Assert.assertEquals(assignments_by_data_set.getColumn("version_id").getType(), DataType.timeuuid());
    Assert.assertEquals(assignments_by_data_set.getColumn("creation_date").getType(), DataType.timestamp());
    //
    Assert.assertNotNull(assignments_by_representations);
    Assert.assertNotNull(assignments_by_representations.getColumn("cloud_id"));
    Assert.assertNotNull(assignments_by_representations.getColumn("schema_id"));
    Assert.assertNotNull(assignments_by_representations.getColumn("version_id"));
    Assert.assertNotNull(assignments_by_representations.getColumn("provider_dataset_id"));
    Assert.assertNotNull(assignments_by_representations.getColumn("creation_date"));

    Assert.assertEquals(assignments_by_representations.getColumn("cloud_id").getType(), DataType.varchar());
    Assert.assertEquals(assignments_by_representations.getColumn("schema_id").getType(), DataType.varchar());
    Assert.assertEquals(assignments_by_representations.getColumn("version_id").getType(), DataType.timeuuid());
    Assert.assertEquals(assignments_by_representations.getColumn("provider_dataset_id").getType(), DataType.varchar());
    Assert.assertEquals(assignments_by_representations.getColumn("creation_date").getType(), DataType.timestamp());
    //
    ResultSet rs = session.execute("select * from data_set_assignments_by_data_set_buckets");
    Assert.assertTrue(rs.getAvailableWithoutFetching() == 2);
    while (rs.iterator().hasNext()) {
      Row r = rs.iterator().next();
      String provider_dataset_id = r.getString("object_id");
      if (provider_dataset_id.equals("provider_dataset_id1")) {
        Assert.assertTrue(r.getLong("rows_count") == 1);
      } else if (provider_dataset_id.equals("provider_dataset_id2")) {
        Assert.assertTrue(r.getLong("rows_count") == 1);
      } else {
        Assert.assertTrue("Too much data", false);
      }
    }
    //
    rs = session.execute("select * from data_set_assignments_by_representations");
    Assert.assertTrue(rs.getAvailableWithoutFetching() == 2);
    while (rs.iterator().hasNext()) {
      Row r = rs.iterator().next();
      String cloud_id = r.getString("cloud_id");
      if (cloud_id.equals("cloud_id1")) {
        Assert.assertTrue(cloud_id.equals("cloud_id1"));
        Assert.assertTrue(r.getString("schema_id").equals("schema_id1"));
        Assert.assertTrue(r.getUUID("version_id").toString().equals("03e473e0-a201-11e7-a8ab-0242ac110009"));
        Assert.assertTrue(r.getString("provider_dataset_id").equals("provider_dataset_id1"));
      } else if (cloud_id.equals("cloud_id2")) {
        Assert.assertTrue(cloud_id.equals("cloud_id2"));
        Assert.assertTrue(r.getString("schema_id").equals("schema_id2"));
        Assert.assertTrue(r.getUUID("version_id").toString().equals("03e473e0-a201-11e7-a8ab-0242ac110019"));
        Assert.assertTrue(r.getString("provider_dataset_id").equals("provider_dataset_id2"));
      } else {
        Assert.assertTrue("Too much data", false);
      }
    }
    //
    rs = session.execute("select * from data_set_assignments_by_data_set");
    Assert.assertTrue(rs.getAvailableWithoutFetching() == 2);
    while (rs.iterator().hasNext()) {
      Row r = rs.iterator().next();
      String cloud_id = r.getString("cloud_id");
      if (cloud_id.equals("cloud_id1")) {
        Assert.assertTrue(r.getString("provider_dataset_id").equals("provider_dataset_id1"));
        Assert.assertTrue(r.getString("schema_id").equals("schema_id1"));
        Assert.assertTrue(r.getString("cloud_id").equals("cloud_id1"));
        Assert.assertTrue(r.getUUID("version_id").toString().equals("03e473e0-a201-11e7-a8ab-0242ac110009"));
      } else if (cloud_id.equals("cloud_id2")) {
        Assert.assertTrue(r.getString("provider_dataset_id").equals("provider_dataset_id2"));
        Assert.assertTrue(r.getString("schema_id").equals("schema_id2"));
        Assert.assertTrue(r.getString("cloud_id").equals("cloud_id2"));
        Assert.assertTrue(r.getUUID("version_id").toString().equals("03e473e0-a201-11e7-a8ab-0242ac110019"));
      } else {
        Assert.assertTrue("Too much data", false);
      }
    }
  }
}
