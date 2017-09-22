package migrations.service.mcs.V9;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

/**
 * Created by Tarek on 9/22/2017.
 */
public class V9_1__drop_index_representations_provider_id implements JavaMigration {
    @Override
    public void migrate(Session session) throws Exception {
        session.execute(
                "DROP INDEX IF EXISTS representations_provider_id;");
    }
}

