package migrations.service.mcs.V2;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

/**
 * @author krystian.
 */
public class V2_1__create___data_set_representation_names_id_MCS implements JavaMigration {

  @Override
  public void migrate(Session session) {
    session.execute("CREATE TABLE data_set_representation_names(\n" +
        "       provider_id varchar,\n" +
        "       dataset_id varchar,\n" +
        "       representation_names set<text>,\n" +
        "       PRIMARY KEY ((provider_id, dataset_id))\n" +
        ");\n");
  }
}

