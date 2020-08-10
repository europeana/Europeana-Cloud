package migrations.service.dps.V7;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

public class V7_1__create_record_processing_state implements JavaMigration {
    @Override
    public void migrate(Session session) throws Exception {
        session.execute(
                "CREATE TABLE record_processing_state(" +
                        "        task_id bigint," +
                        "        record_id varchar," +
                        "        attempt_number int," +
                        "        start_time timestamp," +
                        "        PRIMARY KEY(task_id,record_id)" +
                        ");"
        );
    }
}
