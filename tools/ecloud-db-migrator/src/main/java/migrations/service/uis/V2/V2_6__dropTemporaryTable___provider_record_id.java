package migrations.service.uis.V2;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

/**
 * @author Tarek.
 */
public class V2_6__dropTemporaryTable___provider_record_id implements JavaMigration {
    @Override
    public void migrate(Session session) {
        session.execute(
                "DROP TABLE provider_record_id_copy;\n");
    }
}

