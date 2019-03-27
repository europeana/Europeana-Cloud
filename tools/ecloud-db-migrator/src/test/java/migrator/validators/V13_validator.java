package migrator.validators;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import migrator.EmbeddedCassandra;
import org.junit.Assert;

/**
 * Created by pwozniak on 3/7/19
 */
public class V13_validator {

    private Session session;

    public V13_validator(Session session) {
        this.session = session;
    }

    public void validate() {
        KeyspaceMetadata meta = session.getCluster().getMetadata().getKeyspace(EmbeddedCassandra.KEYSPACE);
        TableMetadata data_set_assignments_by_revision_id = meta.getTable("data_set_assignments_by_revision_id_v1");

        Assert.assertNotNull(data_set_assignments_by_revision_id);
        //
        assertColumns(data_set_assignments_by_revision_id);
        assertPartitionKey(data_set_assignments_by_revision_id);
        assertClusteringKey(data_set_assignments_by_revision_id);
    }

    private void assertColumns(TableMetadata data_set_assignments_by_revision_id) {
        Assert.assertTrue(data_set_assignments_by_revision_id.getColumns().size() == 11);
        Assert.assertNotNull(data_set_assignments_by_revision_id.getColumn("provider_id"));
        Assert.assertNotNull(data_set_assignments_by_revision_id.getColumn("dataset_id"));
        Assert.assertNotNull(data_set_assignments_by_revision_id.getColumn("bucket_id"));
        Assert.assertNotNull(data_set_assignments_by_revision_id.getColumn("revision_provider_id"));
        Assert.assertNotNull(data_set_assignments_by_revision_id.getColumn("revision_name"));
        Assert.assertNotNull(data_set_assignments_by_revision_id.getColumn("revision_timestamp"));
        Assert.assertNotNull(data_set_assignments_by_revision_id.getColumn("representation_id"));
        Assert.assertNotNull(data_set_assignments_by_revision_id.getColumn("cloud_id"));
        Assert.assertNotNull(data_set_assignments_by_revision_id.getColumn("acceptance"));
        Assert.assertNotNull(data_set_assignments_by_revision_id.getColumn("published"));
        Assert.assertNotNull(data_set_assignments_by_revision_id.getColumn("mark_deleted"));
    }

    private void assertPartitionKey(TableMetadata data_set_assignments_by_revision_id) {
        Assert.assertTrue(data_set_assignments_by_revision_id.getPartitionKey().size() == 3);
        Assert.assertTrue(data_set_assignments_by_revision_id
                .getPartitionKey().contains(data_set_assignments_by_revision_id.getColumn("provider_id")));
        Assert.assertTrue(data_set_assignments_by_revision_id
                .getPartitionKey().contains(data_set_assignments_by_revision_id.getColumn("dataset_id")));
        Assert.assertTrue(data_set_assignments_by_revision_id
                .getPartitionKey().contains(data_set_assignments_by_revision_id.getColumn("bucket_id")));
    }

    private void assertClusteringKey(TableMetadata data_set_assignments_by_revision_id) {
        Assert.assertTrue(data_set_assignments_by_revision_id.getClusteringColumns().size() == 5);
        Assert.assertTrue(data_set_assignments_by_revision_id
                .getClusteringColumns().contains(data_set_assignments_by_revision_id.getColumn("representation_id")));
        Assert.assertTrue(data_set_assignments_by_revision_id
                .getClusteringColumns().contains(data_set_assignments_by_revision_id.getColumn("revision_name")));
        Assert.assertTrue(data_set_assignments_by_revision_id
                .getClusteringColumns().contains(data_set_assignments_by_revision_id.getColumn("revision_provider_id")));
        Assert.assertTrue(data_set_assignments_by_revision_id
                .getClusteringColumns().contains(data_set_assignments_by_revision_id.getColumn("revision_timestamp")));
        Assert.assertTrue(data_set_assignments_by_revision_id
                .getClusteringColumns().contains(data_set_assignments_by_revision_id.getColumn("cloud_id")));
    }


}
