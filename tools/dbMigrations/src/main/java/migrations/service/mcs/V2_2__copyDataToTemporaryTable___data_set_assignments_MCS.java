package migrations.service.mcs;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.*;
import migrations.common.CopyTable;

/**
 * @author krystian.
 */
public class V2_2__copyDataToTemporaryTable___data_set_assignments_MCS extends CopyTable implements JavaMigration {

    private final String sourceTable = "data_set_assignments";
    private final String targetTable = "data_set_assignments_copy";

    private final String select = "SELECT "
            + " provider_dataset_id, cloud_id, schema_id, version_id, creation_date "
            + " FROM " + sourceTable +" ;\n";
    private final String insert = "INSERT INTO "
            + targetTable + " (provider_dataset_id, cloud_id, schema_id, version_id, " +
            "creation_date) "
            + "VALUES (?,?,?,?,?);\n";

    @Override
    public void migrate(Session session) throws Exception {
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
