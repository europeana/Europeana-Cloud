package migrations.service.mcs.V8;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;


public class V9_1_drop_index_representations_provider_id implements JavaMigration {

    @Override
    public void migrate(Session session) throws Exception {
        session.execute(
                "DROP INDEX IF EXISTS representations_provider_id;");
    }
}