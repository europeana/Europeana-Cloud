package migrations.service.mcs.V12;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

/**
 * Created by Tarek on 9/22/2017.
 */
public class V12_1__create_new_latest_provider_dataset_representation_revision_table implements JavaMigration {
    @Override
    public void migrate(Session session) throws Exception {

        session.execute(
                "DROP TABLE latest_provider_dataset_representation_revision;");

        session.execute(
                "CREATE TABLE latest_provider_dataset_representation_revision (\n" +
                        "  provider_id varchar,\n" +
                        "  dataset_id varchar,\n" +
                        "  cloud_id varchar,\n" +
                        "  representation_id varchar,\n" +
                        "  revision_timestamp timestamp,\n" +
                        "  revision_name varchar,\n" +
                        "  revision_provider varchar,\n" +
                        "  version_id timeuuid,\n" +
                        "  acceptance boolean,\n" +
                        "  published boolean,\n" +
                        "  mark_deleted boolean,\n" +
                        "  PRIMARY KEY ((provider_id, dataset_id),representation_id,revision_name,revision_provider,mark_deleted,cloud_id)\n" +
                        ");");
    }
}
