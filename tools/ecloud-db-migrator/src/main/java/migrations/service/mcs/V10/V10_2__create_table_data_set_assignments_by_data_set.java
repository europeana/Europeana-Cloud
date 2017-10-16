package migrations.service.mcs.V10;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

public class V10_2__create_table_data_set_assignments_by_data_set implements JavaMigration {


    @Override
    public void migrate(Session session) throws Exception {

        session.execute(
                "CREATE TABLE data_set_assignments_by_data_set (\n" +
                        "provider_dataset_id varchar, /* concatenation: provider_id | dataset_id */\n" +
                        "bucket_id timeuuid,\n" +
                        "cloud_id varchar,\n" +
                        "schema_id varchar,\n" +
                        "version_id timeuuid,\n" +
                        "creation_date timestamp,\n" +
                        "PRIMARY KEY ((provider_dataset_id, bucket_id), schema_id, cloud_id, version_id));");
    }
}
