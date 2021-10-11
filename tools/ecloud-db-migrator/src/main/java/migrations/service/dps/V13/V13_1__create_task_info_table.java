package migrations.service.dps.V13;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

public class V13_1__create_task_info_table implements JavaMigration {
    @Override
    public void migrate(Session session) throws Exception {
        System.out.println("WARNING External migration script for TASK_INFO table must be executed!");
    }
}