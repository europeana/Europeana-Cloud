package migrations.service.mcs;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

/**
 * @author krystian.
 */
public class V2_5__dropTemporaryTable___data_set_assignments_MCS implements JavaMigration {
    @Override
    public void migrate(Session session) {
        session.execute(
                "DROP TABLE data_set_assignments_copy;\n");
    }
}

