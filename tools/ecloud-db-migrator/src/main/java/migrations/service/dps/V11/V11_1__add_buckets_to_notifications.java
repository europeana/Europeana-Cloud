package migrations.service.dps.V11;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

public class V11_1__add_buckets_to_notifications implements JavaMigration {
    @Override
    public void migrate(Session session) throws Exception {
        session.execute("DROP TABLE notifications;");
        session.execute(
                "CREATE TABLE notifications (\n" +
                        "    task_id bigint,\n" +
                        "    bucket_number int,\n" +
                        "    resource_num int,\n" +
                        "    topology_name varchar,\n" +
                        "    resource varchar,\n" +
                        "    state varchar,\n" +
                        "    info_text text,\n" +
                        "    additional_informations text,\n" +
                        "    result_resource varchar,\n" +
                        "    PRIMARY KEY((task_id,bucket_number), resource_num)\n" +
                        ") WITH CLUSTERING ORDER BY (resource_num DESC);"
        );
    }
}