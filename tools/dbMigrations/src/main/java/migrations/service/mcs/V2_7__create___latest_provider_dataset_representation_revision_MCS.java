package migrations.service.mcs;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

/**
 * @author Tarek.
 */
public class V2_7__create___data_set_assignments_by_revision_id_MCS implements JavaMigration {
    @Override
    public void migrate(Session session) {
        session.execute("CREATE TABLE latest_provider_dataset_representation_revision ( \n" +
                "provider_id varchar,\n" +
                "dataset_id varchar,\n" +
                "cloud_id varchar,\n" +
                "representation_id varchar,\n" +
                "revision_timestamp timestamp,\n" +
                "revision_name varchar,\n" +
                "revision_provider varchar,\n" +
                "acceptance boolean,\n" +
                "published boolean,\n" +
                "mark_deleted boolean,\n" +
                "PRIMARY KEY ((provider_id, dataset_id),representation_id,revision_name,revision_provider,cloud_id)\n" +
                ");\n");
        session.execute(
                "CREATE INDEX latest_provider_dataset_representation_revision_acceptance ON latest_provider_dataset_representation_revision (acceptance);";
        session.execute(
                "CREATE INDEX latest_provider_dataset_representation_revision_publishe ON latest_provider_dataset_representation_revision (published);";
        session.execute(
                "CCREATE INDEX latest_provider_dataset_representation_revision_delete ON latest_provider_dataset_representation_revision (mark_deleted);";
    }
}

