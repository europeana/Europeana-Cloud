package migrations.service.dps.V8;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

public class V9_1__add_retry_count_to_basic_info implements JavaMigration {
    @Override
    public void migrate(Session session) throws Exception {
        session.execute("ALTER TABLE basic_info ADD retry_count int;");
    }
}