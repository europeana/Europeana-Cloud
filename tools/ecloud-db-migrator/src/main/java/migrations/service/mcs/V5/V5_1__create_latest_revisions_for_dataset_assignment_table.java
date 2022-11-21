package migrations.service.mcs.V5;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

public class V5_1__create_latest_revisions_for_dataset_assignment_table implements JavaMigration {

  @Override
  public void migrate(Session session) {
    session.execute("CREATE TABLE latest_revisions_for_dataset_assignment (\n" +
        "provider_id text,\n" +
        "dataset_id text,\n" +
        "representation_id text,\n" +
        "revision_name text,\n" +
        "revision_provider_id text,\n" +
        "revision_timestamp timestamp,\n" +
        "cloud_id text,\n" +
        "version_id timeuuid,\n" +
        "PRIMARY KEY ((provider_id, dataset_id, representation_id, cloud_id, revision_name, revision_provider_id))\n" +
        ")WITH comment='Retrieve assignment information based on a known cloud_id and known clustering keys.';\n");
  }
}