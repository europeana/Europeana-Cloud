package migrations.service.dps.V12;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

public class V12_1__update_of_harvested_records_table implements JavaMigration {
    @Override
    public void migrate(Session session) throws Exception {
        session.execute("DROP TABLE harvested_records;");
        session.execute(
                "CREATE TABLE harvested_records (" +
                        "metis_dataset_id varchar," +
                        "bucket_number int," +
                        "record_local_id varchar," +
                        "latest_harvest_date timestamp," +
                        "latest_harvest_md5 uuid," +
                        "published_harvest_date timestamp," +
                        "published_harvest_md5 uuid," +
                        "PRIMARY KEY ((metis_dataset_id, bucket_number), record_local_id)" +
                        ");"
        );
    }
}