package migrations.service.mcs.V14;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

public class V14_1__create_table_data_set_assignments_by_revision_id_replica implements JavaMigration {

    @Override
    public void migrate(Session session) throws Exception {

        session.execute(
                "CREATE TABLE IF NOT EXISTS data_set_assignments_by_revision_id_replica (\n" +
                        "     provider_id varchar,\n" +
                        "     dataset_id varchar,\n" +
                        "     revision_provider_id varchar,\n" +
                        "     revision_name varchar,\n" +
                        "     revision_timestamp timestamp,\n" +
                        "     representation_id varchar,\n" +
                        "     cloud_id varchar,\n" +
                        "     published boolean,\n" +
                        "     acceptance boolean,\n" +
                        "     mark_deleted boolean,\n" +
                       "      PRIMARY KEY ((provider_id, dataset_id), revision_provider_id, revision_name, revision_timestamp, representation_id, cloud_id)\n" +
                        ") WITH comment='Retrieve cloud Ids based on a known provider_id, dataset_id, revision_id';\n");
    }
}




