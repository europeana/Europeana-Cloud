package migrations.service.mcs.V4;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

/**
 * @author krystian.
 */
public class V4_1__create___data_set_assignments_by_revision_id_MCS implements JavaMigration {
    @Override
    public void migrate(Session session) {
        session.execute("CREATE TABLE data_set_assignments_by_revision_id (\n" +
                "provider_id text,\n" +
                "dataset_id text,\n" +
                "revision_provider_id text,\n" +
                "revision_name text,\n" +
                "revision_timestamp timestamp,\n" +
                "representation_id text,\n" +
                "cloud_id text,\n" +
                "acceptance boolean,\n" +
                "mark_deleted boolean,\n" +
                "published boolean,\n" +
                "PRIMARY KEY ((provider_id, dataset_id), revision_provider_id, revision_name, revision_timestamp, representation_id, cloud_id)\n" +
                ");\n");
    }
}