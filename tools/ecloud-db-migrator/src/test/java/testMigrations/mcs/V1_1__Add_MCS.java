package testMigrations.mcs;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

/**
 * @author krystian.
 */
public class V1_1__Add_MCS implements JavaMigration {
    @Override
    public void migrate(Session session) {
        session.execute("INSERT INTO "
                + "data_set_assignments (provider_dataset_id, cloud_id, schema_id, version_id, creation_date) "
                + "VALUES ('provider_dataset_id1','cloud_id1','schema_id1',now(), dateof( now() ) );");
        session.execute("INSERT INTO "
                + "data_set_assignments (provider_dataset_id, cloud_id, schema_id, version_id, creation_date) "
                + "VALUES ('provider_dataset_id2','cloud_id2','schema_id2',now(), dateof( now() ) );");
    }
}
