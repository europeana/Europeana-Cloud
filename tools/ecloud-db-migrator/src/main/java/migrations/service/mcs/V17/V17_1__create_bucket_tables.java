package migrations.service.mcs.V17;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

/**
 * Created by pwozniak on 4/24/19
 */
public class V17_1__create_bucket_tables implements JavaMigration {

  @Override
  public void migrate(Session session) throws Exception {
    session.execute(
        "CREATE TABLE latest_dataset_representation_revision_v1 (\n" +
            "provider_id varchar,\n" +
            "dataset_id varchar,\n" +
            "bucket_id timeuuid,\n" +
            "cloud_id varchar,\n" +
            "representation_id varchar,\n" +
            "revision_timestamp timestamp,\n" +
            "revision_name varchar,\n" +
            "revision_provider varchar,\n" +
            "version_id timeuuid,\n" +
            "acceptance boolean,\n" +
            "published boolean,\n" +
            "mark_deleted boolean,\n" +
            "PRIMARY KEY ((provider_id, dataset_id, bucket_id),representation_id,revision_name,revision_provider,mark_deleted,cloud_id));\n"
    );

    session.execute(
        "CREATE TABLE latest_dataset_representation_revision_buckets (\n" +
            "object_id varchar,\n" +
            "bucket_id timeuuid,\n" +
            "rows_count counter,\n" +
            "PRIMARY KEY (object_id, bucket_id));\n");
  }
}
