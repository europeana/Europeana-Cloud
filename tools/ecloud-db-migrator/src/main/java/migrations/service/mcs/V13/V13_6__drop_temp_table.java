package migrations.service.mcs.V13;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

public class V13_6__drop_temp_table implements JavaMigration {

    @Override
    public void migrate(Session session) throws Exception {
        session.execute("DROP TABLE data_set_assignments_by_revision_id_temp;\n");
    }
}
