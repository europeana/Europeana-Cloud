package migrations.service.mcs;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

/**
 * @author krystian.
 */
public class V1__Initial_MCS implements JavaMigration {
    @Override
    public void migrate(Session session) throws Exception {
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
                    "provider_dataset_id varchar, /* concatenation: provider_id | dataset_id */\n" +
                    "cloud_id varchar,\n" +
                    "schema_id varchar,\n" +
                    "version_id timeuuid,\n" +
                    "creation_date timestamp,\t\n" +
                    "PRIMARY KEY (cloud_id, schema_id, provider_dataset_id, version_id)\n" +
                ");\n");

        session.execute(
                "CREATE INDEX data_set_assignments_provider_dataset_id ON data_set_assignments (provider_dataset_id);\n");

        session.execute(
                "CREATE TABLE representation_versions (\n" +
                    "cloud_id varchar,\n" +
                    "schema_id varchar,\n" +
                    "version_id timeuuid,\n" +
                    "provider_id varchar,\n" +
                    "persistent boolean,\n" +
                    "files map<varchar, text>,  /* fileName -> json object: (mime_type, content_md5, content_length, last_modification_date) */\n" +
                    "revisions map<varchar, text>,\n" +
                    "creation_date timestamp,\n" +
                    "PRIMARY KEY (cloud_id, schema_id, version_id)\n" +
                ");\n");

        session.execute(
                "CREATE INDEX representations_provider_id ON representation_versions (provider_id);\n");

        session.execute(
                "CREATE TABLE data_set_representation_names(\n" +
                    "provider_id varchar,\n" +
                    "dataset_id varchar,\n" +
                    "representation_names set<text>,\n" +
                    "PRIMARY KEY ((provider_id, dataset_id))\n" +
                    ")WITH comment='Retrieve information about the representations supported in a provider’s " +
                        "dataset';\n");
    }
}
