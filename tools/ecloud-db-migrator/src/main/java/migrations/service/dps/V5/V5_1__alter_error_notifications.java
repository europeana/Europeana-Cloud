package migrations.service.dps.V5;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

public class V5_1__alter_error_notifications implements JavaMigration {

  @Override
  public void migrate(Session session) throws Exception {
    session.execute("ALTER TABLE error_notifications ADD additional_informations text;");
  }
}