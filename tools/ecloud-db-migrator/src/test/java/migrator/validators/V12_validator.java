package migrator.validators;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import migrator.EmbeddedCassandra;
import org.junit.Assert;

/**
 * Created by pwozniak on 5/22/18
 */
public class V12_validator {

  private Session session;

  public V12_validator(Session session) {
    this.session = session;
  }

  public void validate() {
    KeyspaceMetadata meta = session.getCluster().getMetadata().getKeyspace(EmbeddedCassandra.KEYSPACE);
    TableMetadata latest_provider_dataset_representation_revision = meta.getTable(
        "latest_provider_dataset_representation_revision");

    Assert.assertNotNull(latest_provider_dataset_representation_revision);
    //
    assertColumns(latest_provider_dataset_representation_revision);
    assertPartitionKey(latest_provider_dataset_representation_revision);
    assertClusteringKey(latest_provider_dataset_representation_revision);
  }

  private void assertColumns(TableMetadata latest_provider_dataset_representation_revision) {
    Assert.assertTrue(latest_provider_dataset_representation_revision.getColumns().size() == 11);
    Assert.assertNotNull(latest_provider_dataset_representation_revision.getColumn("provider_id"));
    Assert.assertNotNull(latest_provider_dataset_representation_revision.getColumn("dataset_id"));
    Assert.assertNotNull(latest_provider_dataset_representation_revision.getColumn("cloud_id"));
    Assert.assertNotNull(latest_provider_dataset_representation_revision.getColumn("representation_id"));
    Assert.assertNotNull(latest_provider_dataset_representation_revision.getColumn("revision_timestamp"));
    Assert.assertNotNull(latest_provider_dataset_representation_revision.getColumn("revision_name"));
    Assert.assertNotNull(latest_provider_dataset_representation_revision.getColumn("revision_provider"));
    Assert.assertNotNull(latest_provider_dataset_representation_revision.getColumn("version_id"));
    Assert.assertNotNull(latest_provider_dataset_representation_revision.getColumn("acceptance"));
    Assert.assertNotNull(latest_provider_dataset_representation_revision.getColumn("published"));
    Assert.assertNotNull(latest_provider_dataset_representation_revision.getColumn("mark_deleted"));
  }

  private void assertPartitionKey(TableMetadata latest_provider_dataset_representation_revision) {
    Assert.assertTrue(latest_provider_dataset_representation_revision.getPartitionKey().size() == 2);
    Assert.assertTrue(latest_provider_dataset_representation_revision
        .getPartitionKey().contains(latest_provider_dataset_representation_revision.getColumn("provider_id")));
    Assert.assertTrue(latest_provider_dataset_representation_revision
        .getPartitionKey().contains(latest_provider_dataset_representation_revision.getColumn("dataset_id")));
  }

  private void assertClusteringKey(TableMetadata latest_provider_dataset_representation_revision) {
    Assert.assertTrue(latest_provider_dataset_representation_revision.getClusteringColumns().size() == 5);
    Assert.assertTrue(latest_provider_dataset_representation_revision
        .getClusteringColumns().contains(latest_provider_dataset_representation_revision.getColumn("representation_id")));
    Assert.assertTrue(latest_provider_dataset_representation_revision
        .getClusteringColumns().contains(latest_provider_dataset_representation_revision.getColumn("revision_name")));
    Assert.assertTrue(latest_provider_dataset_representation_revision
        .getClusteringColumns().contains(latest_provider_dataset_representation_revision.getColumn("revision_provider")));
    Assert.assertTrue(latest_provider_dataset_representation_revision
        .getClusteringColumns().contains(latest_provider_dataset_representation_revision.getColumn("mark_deleted")));
    Assert.assertTrue(latest_provider_dataset_representation_revision
        .getClusteringColumns().contains(latest_provider_dataset_representation_revision.getColumn("cloud_id")));
  }
}
