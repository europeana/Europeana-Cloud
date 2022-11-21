package testMigrations.uis;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

/**
 * @author krystian.
 */
public class V1_1__Add implements JavaMigration {

  @Override
  public void migrate(Session session) {
    session.execute("INSERT INTO "
        + "provider_record_id  (provider_id, record_id, cloud_id) "
        + "VALUES ('provider_id','record_id','cloud_id');");

  }

}
