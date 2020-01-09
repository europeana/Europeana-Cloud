package migrations.service.dps.V6;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

public class V6_1__prep_db_for_topology_by_record implements JavaMigration {
    @Override
    public void migrate(Session session) throws Exception {
        session.execute(
                "CREATE TABLE tasks_by_task_state(" +
                        "        state varchar," +
                        "        topology_name varchar," +
                        "        task_id bigint," +
                        "        application_id varchar," +
                        "        topic_name varchar," +
                        "        start_time timestamp," +
                        "        PRIMARY KEY(state,topology_name,task_id)" +
                        ");"
        );
        session.execute(
                "CREATE TABLE processed_records(" +
                        "        task_id bigint," +
                        "        src_identifier varchar," +
                        "        dst_identifier varchar," +
                        "        topology_name varchar," +
                        "        state varchar," +
                        "        info_text text," +
                        "        additional_informations text," +
                        "        PRIMARY KEY(task_id, src_identifier)" +
                        ");"
        );
        session.execute("ALTER TABLE basic_info ADD task_informations text;");
    }
}
