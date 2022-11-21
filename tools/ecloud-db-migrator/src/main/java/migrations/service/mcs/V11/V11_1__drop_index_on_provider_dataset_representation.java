package migrations.service.mcs.V11;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

public class V11_1__drop_index_on_provider_dataset_representation implements JavaMigration {

  @Override
  public void migrate(Session session) throws Exception {

    session.execute(
        "DROP INDEX IF EXISTS provider_dataset_representation_acceptance;");

    session.execute(
        "DROP INDEX IF EXISTS dataset_representation_published;");

    session.execute(
        "DROP INDEX IF EXISTS provider_dataset_representation_mark_deleted;");

    session.execute(
        "DROP INDEX IF EXISTS provider_dataset_representation_revision_id;");
  }
}
