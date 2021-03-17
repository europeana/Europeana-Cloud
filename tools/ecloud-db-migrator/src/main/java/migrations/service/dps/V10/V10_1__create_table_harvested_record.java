package migrations.service.dps.V8;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

public class V10_1__create_table_harvested_record implements JavaMigration {
    @Override
    public void migrate(Session session) throws Exception {
        session.execute("create table harvested_record(\n" +
                "provider_id varchar,\n" +
                "dataset_id varchar,\n" +
                "bucket_number int, \n" +
                "oai_id varchar,\n" +
                "harvest_date timestamp,\n" +
                "indexing_date timestamp,\n" +
                "md5 uuid, \n" +
                "ignored boolean,\n" +
                "PRIMARY KEY ((provider_id,dataset_id,oai_id_bucket_no), oai_id));");
    }
}