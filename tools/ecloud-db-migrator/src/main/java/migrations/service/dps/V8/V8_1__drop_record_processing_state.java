package migrations.service.dps.V8;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

public class V8_1__drop_record_processing_state implements JavaMigration {

  @Override
  public void migrate(Session session) throws Exception {
    session.execute(
        "DROP TABLE record_processing_state;"
    );
  }
}
