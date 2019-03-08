package migrations.service.mcs.V13;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

public class V13_4__change_table_schema implements JavaMigration {

    @Override
    public void migrate(Session session) throws Exception {
        session.execute(
                "DROP TABLE data_set_assignments_by_revision_id;\n");

        session.execute(
                "CREATE TABLE data_set_assignments_by_revision_id (\n" +
                        "       provider_id varchar,\n" +
                        "       dataset_id varchar,\n" +
                        "       bucket_id timeuuid,\n" +
                        "       revision_provider_id varchar,\n" +
                        "       revision_name varchar,\n" +
                        "       revision_timestamp timestamp,\n" +
                        "       representation_id varchar,\n" +
                        "       cloud_id varchar,\n" +
                        "       published boolean,\n" +
                        "       acceptance boolean,\n" +
                        "       mark_deleted boolean,\n" +
                        "       PRIMARY KEY ((provider_id, dataset_id, bucket_id), revision_provider_id, revision_name, revision_timestamp, representation_id, cloud_id)\n" +
                        ")WITH comment='Retrieve cloud Ids based on a known provider_id, dataset_id, revision_id';\n");

    }
}