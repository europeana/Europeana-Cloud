package migrations.service.mcs.V10;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

public class V10_3__create_table_data_set_assignments_by_representations implements JavaMigration {

  @Override
  public void migrate(Session session) throws Exception {
    session.execute(
        "CREATE TABLE data_set_assignments_by_representations (\n" +
            "cloud_id varchar,\n" +
            "schema_id varchar,\n" +
            "version_id timeuuid,\n" +
            "provider_dataset_id varchar, /* concatenation: provider_id | dataset_id */\n" +
            "creation_date timestamp,\n" +
            "PRIMARY KEY ((cloud_id, schema_id), version_id, provider_dataset_id));");
  }
}