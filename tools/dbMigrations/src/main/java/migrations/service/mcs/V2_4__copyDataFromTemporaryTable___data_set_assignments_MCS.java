package migrations.service.mcs;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import migrations.common.CopyTable;

/**
 * @author krystian.
 */
public class V2_4__copyDataFromTemporaryTable___data_set_assignments_MCS extends CopyTable implements JavaMigration {

    private static final String sourceTable = "data_set_assignments_copy";
    private static final String targetTable = "data_set_assignments";

    private static final String select = "SELECT "
            + " provider_dataset_id, cloud_id, schema_id, version_id, creation_date "
            + " FROM " + sourceTable +" ;\n";
    private static final String insert = "INSERT INTO "
            + targetTable + " (provider_dataset_id, cloud_id, schema_id, version_id, " +
            "creation_date) "
            + "VALUES (?,?,?,?,?);\n";

    @Override
    public void migrate(Session session) {
        PreparedStatement selectStatement = session.prepare(select);
        PreparedStatement insertStatement = session.prepare(insert);
        copyTable(session, selectStatement, insertStatement);
    }

    @Override
    public void insert(PreparedStatement insertStatement, Row r, Session session) {
        session.execute(insertStatement.bind(
                r.getString("provider_dataset_id"),
                r.getString("cloud_id"),
                r.getString("schema_id"),
                r.getUUID("version_id"),
                r.getDate("creation_date")));
    }
}
