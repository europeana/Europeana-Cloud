package migrations.service.mcs.V7;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

/**
 * Created by pwozniak on 3/31/17.
 */
public class V7_1__create_revisions_related_tables implements JavaMigration {

    @Override
    public void migrate(Session session) throws Exception {
        session.execute(
                "CREATE TABLE provider_dataset_representation (\n" +
                        "provider_id text,\n" +
                        "dataset_id text,\n" +
                        "bucket_id timeuuid,\n" +
                        "representation_id text,\n" +
                        "revision_timestamp timestamp,\n" +
                        "cloud_id text,\n" +
                        "acceptance boolean,\n" +
                        "mark_deleted boolean,\n" +
                        "published boolean,\n" +
                        "revision_id text,\n" +
                        "version_id timeuuid,\n" +
                        "PRIMARY KEY ((provider_id, dataset_id, bucket_id), representation_id, revision_timestamp, cloud_id))");

        session.execute("CREATE INDEX provider_dataset_representation_acceptance ON provider_dataset_representation (acceptance);");
        session.execute("CREATE INDEX provider_dataset_representation_mark_deleted ON provider_dataset_representation (mark_deleted);");
        session.execute("CREATE INDEX dataset_representation_published ON provider_dataset_representation (published);");
        session.execute("CREATE INDEX provider_dataset_representation_revision_id ON provider_dataset_representation (revision_id);");

        ////////////////


        session.execute(
                "CREATE TABLE latest_provider_dataset_representation_revision (\n" +
                        "provider_id text,\n" +
                        "dataset_id text,\n" +
                        "representation_id text,\n" +
                        "revision_name text,\n" +
                        "revision_provider text,\n" +
                        "cloud_id text,\n" +
                        "acceptance boolean,\n" +
                        "mark_deleted boolean,\n" +
                        "published boolean,\n" +
                        "revision_timestamp timestamp,\n" +
                        "version_id timeuuid,\n" +
                        "PRIMARY KEY ((provider_id, dataset_id), representation_id, revision_name, revision_provider, cloud_id))");

        session.execute("CREATE INDEX latest_provider_dataset_representation_revision_acceptance ON latest_provider_dataset_representation_revision (acceptance);");
        session.execute("CREATE INDEX latest_provider_dataset_representation_revision_delete ON latest_provider_dataset_representation_revision (mark_deleted);");
        session.execute("CREATE INDEX latest_provider_dataset_representation_revision_publishe ON latest_provider_dataset_representation_revision (published);");

        /////////////////////////////

        session.execute(
                "CREATE TABLE representation_revisions (\n" +
                        "        cloud_id text,\n" +
                        "        representation_id text,\n" +
                        "        revision_provider_id text,\n" +
                        "        revision_name text,\n" +
                        "        revision_timestamp timestamp,\n" +
                        "        version_id timeuuid,\n" +
                        "        files map<text, text>,\n" +
                        "        PRIMARY KEY ((cloud_id, representation_id), revision_provider_id, revision_name, revision_timestamp, version_id)\n" +
                        "        )");

        ///////////////////////////////

        session.execute(
                "CREATE TABLE datasets_buckets (\n" +
                        "        provider_id text,\n" +
                        "        dataset_id text,\n" +
                        "        bucket_id timeuuid,\n" +
                        "        rows_count counter,\n" +
                        "        PRIMARY KEY (provider_id, dataset_id, bucket_id)\n" +
                        "        )");
    }
}






