package migrations.service.dps.V4;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

public class V4_1__create_statistics_reports_table implements JavaMigration {
    @Override
    public void migrate(Session session) {
        session.execute("CREATE TABLE statistics_reports (\n" +
                "    task_id bigint,\n" +
                "    report_data blob,\n" +
                "    PRIMARY KEY(task_id)\n" +
                ");\n");
    }
}