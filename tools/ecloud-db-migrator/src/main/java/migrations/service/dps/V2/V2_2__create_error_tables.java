package migrations.service.dps.V2;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

public class V2_2__create_error_tables implements JavaMigration {

  @Override
  public void migrate(Session session) {
    session.execute("CREATE TABLE error_notifications (\n" +
        "    task_id bigint,\n" +
        "    error_type timeuuid,\n" +
        "    error_message text,\n" +
        "    resource varchar,\n" +
        "    PRIMARY KEY((task_id,error_type),resource)\n" +
        ");\n");
    session.execute("CREATE TABLE error_counters (\n" +
        "    task_id bigint,\n" +
        "    error_type timeuuid,\n" +
        "    error_count counter,\n" +
        "    PRIMARY KEY(task_id,error_type)\n" +
        ");\n");
  }
}
