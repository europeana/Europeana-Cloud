package migrations.service.mcs;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

/**
 * @author krystian.
 */
public class V2_6__create___data_set_assignments_by_revision_id_MCS implements JavaMigration {
    @Override
    public void migrate(Session session) {
        session.execute("CREATE TABLE data_set_assignments_by_revision_id (\n" +
                "provider_id varchar,\n" +
                "dataset_id varchar,\n" +
                "revision_id varchar,\n" +
                "representation_id varchar,\n" +
                "cloud_id varchar,\n" +
                "PRIMARY KEY ((provider_id, dataset_id), representation_id, revision_id, cloud_id)\n" +
                ")WITH comment='Retrieve cloud Ids based on a known provider_id, dataset_id, revision_id';\n");
        session.execute(
                "CREATE INDEX data_set_assignments_by_revision_id_provider_dataset_id ON " +
                        "data_set_assignments_by_revision_id (provider_dataset_id);\n");
    }
}

