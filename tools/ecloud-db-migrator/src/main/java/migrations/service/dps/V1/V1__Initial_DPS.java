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
                        "    topology_name text,\n" +
                        "    resource text,\n" +
                        "    additional_informations text,\n" +
                        "    info_text text,\n" +
                        "    result_resource text,\n" +
                        "    state text,\n" +
                        "    PRIMARY KEY (task_id, topology_name, resource)\n" +
                        ") WITH CLUSTERING ORDER BY (topology_name ASC, resource ASC);\n");

        session.execute(
                "CREATE TABLE basic_info (\n" +
                        "    task_id bigint,\n" +
                        "    topology_name text,\n" +
                        "    expected_size int,\n" +
                        "    info text,\n" +
                        "    state text,\n" +
                        "    PRIMARY KEY (task_id, topology_name)\n" +
                        ") WITH CLUSTERING ORDER BY (topology_name ASC);\n");


    }
}
