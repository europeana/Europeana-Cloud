package migrations.service.dps.V4;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

public class V4_1__create_statistics_reports_table implements JavaMigration {
    @Override
    public void migrate(Session session) throws Exception {
        session.execute("ALTER TABLE error_notifications ADD additional_informations text;");
    }
}