package migrations.service.dps.V2;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

public class V2_1__add_columns_for_errors implements JavaMigration {
    @Override
    public void migrate(Session session) throws Exception {
        session.execute("ALTER TABLE basic_info ADD errors int;");
    }

}
