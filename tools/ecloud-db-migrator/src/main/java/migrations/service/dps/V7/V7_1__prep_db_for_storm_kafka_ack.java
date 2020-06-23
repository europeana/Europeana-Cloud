package migrations.service.dps.V6;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

public class V6_1__prep_db_for_topology_by_record implements JavaMigration {
    @Override
    public void migrate(Session session) throws Exception {
        session.execute(
                "CREATE TABLE record_processing_state(" +
                        "        task_id bigint," +
                        "        record_id varchar," +
                        "        topology_name varchar," +
                        "        attempt_number int," +
                        "        start_time timestamp," +
                        "        PRIMARY KEY(task_id,record_id,topology_name)" +
                        ");"
        );
    }
}
