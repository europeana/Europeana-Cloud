package migrations.service.mcs.V13;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import migrations.common.TableCopier;

public class V13_3__copy_data extends TableCopier implements JavaMigration {

    private static final String sourceTable = "data_set_assignments_by_revision_id";
    private static final String targetTable = "data_set_assignments_by_revision_id_temp";

    private static final String select = "SELECT "
            + " provider_id, dataset_id, revision_provider_id, revision_name, revision_timestamp, representation_id, cloud_id, published, acceptance, mark_deleted "
            + " FROM " + sourceTable +" ;\n";

    private static final String insert = "INSERT INTO "
            + targetTable + " (provider_id, dataset_id, revision_provider_id, revision_name, revision_timestamp, representation_id, cloud_id, published, acceptance, mark_deleted) "
            + "VALUES (?,?,?,?,?,?,?,?,?,?);\n";


    @Override
    public void migrate(Session session) throws Exception {
        PreparedStatement selectStatement = session.prepare(select);
        PreparedStatement insertStatement = session.prepare(insert);
        copyTable(session, selectStatement, insertStatement);
    }

    @Override
    public void insert(PreparedStatement insertStatement, Row r, Session session) {
        session.execute(insertStatement.bind(
                r.getString("provider_id"),
                r.getString("dataset_id"),
                r.getString("revision_provider_id"),
                r.getString("revision_name"),
                r.getDate("revision_timestamp"),
                r.getString("representation_id"),
                r.getString("cloud_id"),
                r.getBool("published"),
                r.getBool("acceptance"),
                r.getBool("mark_deleted")));
    }
}