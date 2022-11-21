package migrations.service.mcs.V1;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

/**
 * @author krystian.
 */
public class V1__Initial_MCS implements JavaMigration {

  @Override
  public void migrate(Session session) {
    session.execute(
        "CREATE TABLE data_sets (\n" +
            "provider_id varchar,\n" +
            "dataset_id varchar,\n" +
            "description text,\n" +
            "creation_date timestamp, \n" +
            "PRIMARY KEY (provider_id, dataset_id)\n" +
            ");\n");

    session.execute(
        "CREATE TABLE data_set_assignments (\n" +
            "    cloud_id text,\n" +
            "    schema_id text,\n" +
            "    provider_dataset_id text,\n" +
            "    creation_date timestamp,\n" +
            "    version_id timeuuid,\n" +
            "    PRIMARY KEY (cloud_id, schema_id, provider_dataset_id)\n" +
            ") WITH CLUSTERING ORDER BY (schema_id ASC, provider_dataset_id ASC);\n");

    session.execute(
        "CREATE INDEX data_set_assignments_provider_dataset_id ON data_set_assignments (provider_dataset_id);\n");

    session.execute(
        "CREATE TABLE representation_versions (\n" +
            "    cloud_id text,\n" +
            "    schema_id text,\n" +
            "    version_id timeuuid,\n" +
            "    creation_date timestamp,\n" +
            "    files map<text, text>,\n" +
            "    persistent boolean,\n" +
            "    provider_id text,\n" +
            "    PRIMARY KEY (cloud_id, schema_id, version_id)\n" +
            ") WITH CLUSTERING ORDER BY (schema_id ASC, version_id ASC);\n");

    session.execute(
        "CREATE INDEX representations_provider_id ON representation_versions (provider_id);\n");
  }
}
