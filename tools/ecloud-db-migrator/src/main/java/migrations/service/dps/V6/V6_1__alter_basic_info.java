package migrations.service.dps.V6;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

public class V6_1__alter_basic_info implements JavaMigration {
    @Override
    public void migrate(Session session) throws Exception {
        session.execute(
                "CREATE TABLE tasks_by_task_state(" +
                        "        state varchar," +
                        "        topology_name varchar," +
                        "        task_id bigint," +
                        "        application_id varchar," +
                        "        start_time timestamp," +
                        "        PRIMARY KEY(state,topology_name,task_id)" +
                        ");"
        );
        session.execute("ALTER TABLE basic_info ADD task_informations text;");
    }
}
