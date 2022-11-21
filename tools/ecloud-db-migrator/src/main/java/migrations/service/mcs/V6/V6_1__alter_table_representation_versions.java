package migrations.service.mcs.V6;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

/**
 * Created by pwozniak on 3/31/17.
 */
public class V6_1__alter_table_representation_versions implements JavaMigration {

  @Override
  public void migrate(Session session) throws Exception {
    session.execute("ALTER TABLE representation_versions ADD revisions map<text, text>;\n");
  }
}