package migrations.service.mcs.V13;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

public class V13_1__create_table_data_set_assignments_by_revision_id_buckets implements JavaMigration {

    @Override
    public void migrate(Session session) throws Exception {

        session.execute(
                "CREATE TABLE data_set_assignments_by_revision_id_buckets (\n" +
                        "      object_id varchar,\n" +
                        "      bucket_id timeuuid,\n" +
                        "      rows_count counter,\n" +
                        "      PRIMARY KEY (object_id, bucket_id)\n" +
                        ") WITH comment='Keep track of number of rows in a bucket for provider and dataset id assignments.';\n");
    }
}
