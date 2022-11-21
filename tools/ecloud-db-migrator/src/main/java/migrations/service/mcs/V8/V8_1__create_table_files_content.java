package migrations.service.mcs.V8;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

/**
 * Created by pwozniak on 3/31/17.
 */
public class V8_1__create_table_files_content implements JavaMigration {

  @Override
  public void migrate(Session session) throws Exception {
    session.execute(
        "CREATE TABLE files_content (\n" +
            "fileName text PRIMARY KEY,\n" +
            "data blob\n" +
            ")");
  }
}