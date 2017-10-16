package migrations.service.mcs.V10;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

public class V10_1__create_table_data_set_assignments_by_dataset_buckets implements JavaMigration {

    @Override
    public void migrate(Session session) throws Exception {

        session.execute(
                "CREATE TABLE data_set_assignments_by_data_set_buckets (\n" +
                        "provider_dataset_id varchar,\n" +
                        "bucket_id timeuuid,\n" +
                        "rows_count counter,\n" +
                        "PRIMARY KEY (provider_dataset_id, bucket_id)\n" +
                        ") WITH comment='Keep track of number of rows in a bucket for provider and dataset id assignments.';");
    }
}
