package migrations.service.mcs.V3;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

/**
 * @author krystian.
 */
public class V3_1__addTemporaryTable_data_set_assignments_MCS implements JavaMigration {

  @Override
  public void migrate(Session session) {
    session.execute(
        "CREATE TABLE data_set_assignments_copy (\n" +
            "provider_dataset_id varchar, /* concatenation: provider_id | dataset_id */\n" +
            "cloud_id varchar,\n" +
            "schema_id varchar,\n" +
            "version_id timeuuid,\n" +
            "creation_date timestamp,\t\n" +
            "PRIMARY KEY (cloud_id, schema_id, version_id, provider_dataset_id)\n" +
            ");\n");
  }
}
