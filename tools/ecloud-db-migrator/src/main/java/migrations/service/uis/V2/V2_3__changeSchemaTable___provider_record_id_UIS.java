package migrations.service.uis.V2;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

/**
 * @author Tarek.
 */
public class V2_3__changeSchemaTable___provider_record_id_UIS implements JavaMigration {
    @Override
    public void migrate(Session session) {
        session.execute(
                "DROP TABLE provider_record_id;\n");
        session.execute(
                "CREATE TABLE provider_record_id (\n" +
                        "provider_id varchar, \n" +
                        "bucket_id timeuuid,\n" +
                        "record_id varchar,\n" +
                        "cloud_id varchar,\n" +
                        "PRIMARY KEY ((provider_id, bukect_id),record_id)\n" +
                        ");\n");
    }
}


