package migrations.service.dps.V13;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

public class V13_1__create_task_info_table implements JavaMigration {
    @Override
    public void migrate(Session session) throws Exception {
        session.execute(
                "CREATE TABLE task_info (\n" +
                        "    task_id                 bigint,\n" +
                        "    topology_name           varchar,\n" +
                        "    state                   varchar,\n" +
                        "    state_description       varchar,\n" +
                        "    sent_time               timestamp,\n" +
                        "    start_time              timestamp,\n" +
                        "    finish_time             timestamp,\n" +
                        "    expected_records_number int,\n" +
                        "    processed_records_count int,\n" +
                        "    ignored_records_count   int,\n" +
                        "    deleted_records_count   int,\n" +
                        "    processed_errors_count  int,\n" +
                        "    deleted_errors_count    int,\n" +
                        "    definition              text,\n" +
                        "    PRIMARY KEY (task_id)\n" +
                        ");"
        );
    }
}