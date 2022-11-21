package migrations.service.mcs.V10;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

public class V10_5__drop_old_table implements JavaMigration {

  @Override
  public void migrate(Session session) throws Exception {
    session.execute("DROP TABLE data_set_assignments;");
  }
}
