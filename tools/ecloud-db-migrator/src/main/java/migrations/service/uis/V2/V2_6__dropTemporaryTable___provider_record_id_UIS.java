package migrations.service.uis.V2;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

/**
 * @author krystian.
 */
public class V2_6__dropTemporaryTable___provider_record_id_UIS implements JavaMigration {
    @Override
    public void migrate(Session session) {
        session.execute(
                "DROP TABLE provider_record_id_copy;\n");
    }
}

