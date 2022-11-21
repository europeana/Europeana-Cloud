package migrator.validators;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import migrator.EmbeddedCassandra;
import org.junit.Assert;

/**
 * Created by pwozniak on 4/24/19
 */
public class V14_validator {

  private Session session;

  public V14_validator(Session session) {
    this.session = session;
  }

  public void validate() {
    KeyspaceMetadata meta = session.getCluster().getMetadata().getKeyspace(EmbeddedCassandra.KEYSPACE);
    TableMetadata data_set_representation_revision = meta.getTable("latest_dataset_representation_revision_v1");

    Assert.assertNotNull(data_set_representation_revision);
    //
    assertColumns(data_set_representation_revision);
    assertPartitionKey(data_set_representation_revision);
    assertClusteringKey(data_set_representation_revision);
  }

  private void assertColumns(TableMetadata data_set_representation_revision) {
    Assert.assertTrue(data_set_representation_revision.getColumns().size() == 12);
    Assert.assertNotNull(data_set_representation_revision.getColumn("provider_id"));
    Assert.assertNotNull(data_set_representation_revision.getColumn("dataset_id"));
    Assert.assertNotNull(data_set_representation_revision.getColumn("bucket_id"));
    Assert.assertNotNull(data_set_representation_revision.getColumn("cloud_id"));
    Assert.assertNotNull(data_set_representation_revision.getColumn("representation_id"));
    Assert.assertNotNull(data_set_representation_revision.getColumn("revision_timestamp"));
    Assert.assertNotNull(data_set_representation_revision.getColumn("revision_name"));
    Assert.assertNotNull(data_set_representation_revision.getColumn("revision_provider"));
    Assert.assertNotNull(data_set_representation_revision.getColumn("version_id"));
    Assert.assertNotNull(data_set_representation_revision.getColumn("acceptance"));
    Assert.assertNotNull(data_set_representation_revision.getColumn("published"));
    Assert.assertNotNull(data_set_representation_revision.getColumn("mark_deleted"));
  }

  private void assertPartitionKey(TableMetadata data_set_representation_revision) {
    Assert.assertTrue(data_set_representation_revision.getPartitionKey().size() == 3);
    Assert.assertTrue(data_set_representation_revision
        .getPartitionKey().contains(data_set_representation_revision.getColumn("provider_id")));
    Assert.assertTrue(data_set_representation_revision
        .getPartitionKey().contains(data_set_representation_revision.getColumn("dataset_id")));
    Assert.assertTrue(data_set_representation_revision
        .getPartitionKey().contains(data_set_representation_revision.getColumn("bucket_id")));
  }

  private void assertClusteringKey(TableMetadata data_set_assignments_by_revision_id) {
    Assert.assertTrue(data_set_assignments_by_revision_id.getClusteringColumns().size() == 5);
    Assert.assertTrue(data_set_assignments_by_revision_id
        .getClusteringColumns().contains(data_set_assignments_by_revision_id.getColumn("representation_id")));
    Assert.assertTrue(data_set_assignments_by_revision_id
        .getClusteringColumns().contains(data_set_assignments_by_revision_id.getColumn("revision_name")));
    Assert.assertTrue(data_set_assignments_by_revision_id
        .getClusteringColumns().contains(data_set_assignments_by_revision_id.getColumn("revision_provider")));
    Assert.assertTrue(data_set_assignments_by_revision_id
        .getClusteringColumns().contains(data_set_assignments_by_revision_id.getColumn("mark_deleted")));
    Assert.assertTrue(data_set_assignments_by_revision_id
        .getClusteringColumns().contains(data_set_assignments_by_revision_id.getColumn("cloud_id")));
  }
}
