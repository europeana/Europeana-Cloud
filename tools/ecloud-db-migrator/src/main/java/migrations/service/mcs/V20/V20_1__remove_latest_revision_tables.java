package migrations.service.mcs.V20;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;


public class V20_1__remove_latest_revision_tables implements JavaMigration {

    @Override
    public void migrate(Session session) throws Exception {
        session.execute("DROP TABLE latest_dataset_representation_revision_buckets;");
        session.execute("DROP TABLE latest_dataset_representation_revision_v1;");
        session.execute("DROP TABLE latest_revisions_for_dataset_assignment;");
        session.execute("DROP TABLE provider_dataset_representation;");
    }

}

