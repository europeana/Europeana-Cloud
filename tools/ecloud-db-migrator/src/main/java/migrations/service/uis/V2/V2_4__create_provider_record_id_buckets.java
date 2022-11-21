package migrations.service.uis.V2;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

/**
 * @author Tarek.
 */
public class V2_4__create_provider_record_id_buckets implements JavaMigration {

  @Override
  public void migrate(Session session) {
    session.execute(
        "CREATE TABLE provider_record_id_buckets (\n" +
            "object_id varchar,\n" +
            "bucket_id timeuuid, \n" +
            "rows_count counter,\n" +
            "PRIMARY KEY (object_id, bucket_id)\n" +
            ");\n");
  }
}
