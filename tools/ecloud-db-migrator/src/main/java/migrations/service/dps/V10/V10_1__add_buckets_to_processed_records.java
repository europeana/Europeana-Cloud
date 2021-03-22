package migrations.service.dps.V10;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

public class V10_1__add_buckets_to_processed_records implements JavaMigration {
    @Override
    public void migrate(Session session) throws Exception {
        session.execute("DROP TABLE processed_records;");
        session.execute(
                "CREATE TABLE processed_records (" +
                        "task_id bigint," +
                        "bucket_number int," +
                        "record_id varchar," +
                        "attempt_number int," +
                        "dst_identifier varchar," +
                        "topology_name varchar," +
                        "state varchar," +
                        "start_time timestamp," +
                        "info_text text," +
                        "additional_informations text," +
                        "PRIMARY KEY((task_id, bucket_number), record_id)" +
                        ");"
        );
    }
}