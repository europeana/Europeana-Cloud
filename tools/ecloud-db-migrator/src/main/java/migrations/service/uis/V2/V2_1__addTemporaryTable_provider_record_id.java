package migrations.service.uis.V2;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

/**
 * @author Tarek.
 */
public class V2_1__addTemporaryTable_provider_record_id implements JavaMigration {

  @Override
  public void migrate(Session session) {
    session.execute(
        "CREATE TABLE provider_record_id_copy (\n" +
            "provider_id varchar,\n" +
            "record_id varchar,\n" +
            "cloud_id varchar,\n" +
            "PRIMARY KEY (provider_id,record_id)\n" +
            ") WITH CLUSTERING ORDER BY (record_id ASC);\n");
  }
}


