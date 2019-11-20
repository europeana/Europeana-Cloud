package migrations.service.dps.V6;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

public class V6_1__alter_basic_info implements JavaMigration {
    @Override
    public void migrate(Session session) throws Exception {
        session.execute("ALTER TABLE basic_info ADD task_informations text;");
    }
}
