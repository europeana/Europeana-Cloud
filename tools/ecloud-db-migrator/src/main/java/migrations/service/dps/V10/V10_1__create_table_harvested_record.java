package migrations.service.dps.V8;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

public class V10_1__create_table_harvested_record implements JavaMigration {
    @Override
    public void migrate(Session session) throws Exception {
        session.execute("create table harvested_records(\n" +
                "    provider_id     varchar,\n" +
                "    dataset_id      varchar,\n" +
                "    bucket_number   int,\n" +
                "    record_local_id varchar,\n" +
                "    harvest_date    timestamp,\n" +
                "    md5             uuid,\n" +
                "    indexing_date   timestamp,\n" +
                "    PRIMARY KEY ((provider_id, dataset_id, bucket_number), record_local_id)\n" +
                ");");
    }
}