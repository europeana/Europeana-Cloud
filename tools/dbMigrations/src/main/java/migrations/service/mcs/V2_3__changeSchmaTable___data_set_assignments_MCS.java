package migrations.service.mcs;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.*;

/**
 * @author krystian.
 */
public class V2_3__changeSchmaTable___data_set_assignments_MCS implements JavaMigration {
    @Override
    public void migrate(Session session) throws Exception {
        session.execute(
                "DROP TABLE data_set_assignments;\n");
        session.execute(
                "CREATE TABLE data_set_assignments (\n" +
                        "provider_dataset_id varchar, /* concatenation: provider_id | dataset_id */\n" +
                        "cloud_id varchar,\n" +
                        "schema_id varchar,\n" +
                        "version_id timeuuid,\n" +
                        "creation_date timestamp,\t\n" +
                        "PRIMARY KEY (cloud_id, schema_id, version_id, provider_dataset_id)\n" +
                        ");\n");
        session.execute(
                "CREATE INDEX data_set_assignments_provider_dataset_id ON data_set_assignments (provider_dataset_id);\n");
        }
}

