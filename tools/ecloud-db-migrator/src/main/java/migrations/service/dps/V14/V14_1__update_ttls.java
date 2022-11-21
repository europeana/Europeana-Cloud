package migrations.service.dps.V14;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

public class V14_1__update_ttls implements JavaMigration {

  @Override
  public void migrate(Session session) throws Exception {
    session.execute("alter table notifications with default_time_to_live = 2592000;");
    session.execute("alter table processed_records with default_time_to_live = 2592000;");

  }
}