package migrations.service.dps.V1;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

/**
 * @author krystian.
 */
public class V1__Initial_DPS implements JavaMigration {
    @Override
    public void migrate(Session session) {
        session.execute(
                "CREATE TABLE notifications (\n" +
                        "    task_id bigint,\n" +
                        "    resource_num int,\n"+
                        "    topology_name text,\n" +
                        "    resource text,\n" +
                        "    additional_informations text,\n" +
                        "    info_text text,\n" +
                        "    result_resource text,\n" +
                        "    state text,\n" +
                        "    PRIMARY KEY (task_id, resource_num)\n" +
                        ") WITH CLUSTERING ORDER BY (resource_num ASC);\n");

        session.execute(
                "CREATE TABLE basic_info (\n" +
                        "    task_id bigint,\n" +
                        "    topology_name text,\n" +
                        "    expected_size int,\n" +
                        "    finish_time timestamp,\n" +
                        "    info text,\n" +
                        "    processed_files_count int,\n" +
                        "    sent_time timestamp,\n" +
                        "    start_time timestamp,\n" +
                        "    state text,\n" +
                        "    PRIMARY KEY(task_id)\n" +
                        ");\n");
    }
}
