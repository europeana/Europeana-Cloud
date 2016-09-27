package migrations.service.uis.V1;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

/**
 * @author krystian.
 */
public class V1__Initial_UIS implements JavaMigration {
    @Override
    public void migrate(Session session) {
        session.execute(
                "CREATE TABLE data_providers (\n" +
                        "    provider_id text PRIMARY KEY,\n" +
                        "    active boolean,\n" +
                        "    creation_date timestamp,\n" +
                        "    data_sets map<text, text>,\n" +
                        "    partition_key int,\n" +
                        "    properties map<text, text>\n" +
                        ");\n");

        session.execute(
                "CREATE TABLE cloud_id (\n" +
                        "    cloud_id text,\n" +
                        "    provider_id text,\n" +
                        "    record_id text,\n" +
                        "    deleted boolean,\n" +
                        "    PRIMARY KEY (cloud_id, provider_id, record_id)\n" +
                        ") WITH CLUSTERING ORDER BY (provider_id ASC, record_id ASC);\n");

        session.execute(
                "CREATE INDEX deleted_records ON cloud_id (deleted);\n");

        session.execute(
                "CREATE TABLE provider_record_id (\n" +
                        "    provider_id text,\n" +
                        "    record_id text,\n" +
                        "    cloud_id text,\n" +
                        "    deleted boolean,\n" +
                        "    PRIMARY KEY (provider_id, record_id)\n" +
                        ") WITH CLUSTERING ORDER BY (record_id ASC);\n");

        session.execute(
                "CREATE INDEX record_deleted ON provider_record_id (deleted);\n");


    }
}
