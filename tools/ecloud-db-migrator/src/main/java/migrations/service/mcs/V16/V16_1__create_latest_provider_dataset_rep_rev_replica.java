package migrations.service.mcs.V16;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

/**
 * Created by Tarek on 4/26/19
 */
public class V16_1__create_latest_provider_dataset_rep_rev_replica implements JavaMigration {

  @Override
  public void migrate(Session session) throws Exception {
    session.execute(
        "CREATE TABLE IF NOT EXISTS latest_provider_dataset_rep_rev_replica (\n" +
            "provider_id varchar,\n" +
            "dataset_id varchar,\n" +
            "cloud_id varchar,\n" +
            "representation_id varchar,\n" +
            "revision_timestamp timestamp,\n" +
            "revision_name varchar,\n" +
            "revision_provider varchar,\n" +
            "version_id timeuuid,\n" +
            "acceptance boolean,\n" +
            "published boolean,\n" +
            "mark_deleted boolean,\n" +
            "PRIMARY KEY ((provider_id, dataset_id),representation_id,revision_name,revision_provider,mark_deleted,cloud_id));\n"
    );

  }
}
