package migrations.service.uis.V2;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import migrations.common.TableCopier;

/**
 * @author Tarek.
 */
public class V2_2__DataToTemporaryTable___Copier___provider_record_id extends TableCopier implements JavaMigration {

  private static final String sourceTable = "provider_record_id";
  private static final String targetTable = "provider_record_id_copy";

  private static final String select = "SELECT "
      + " provider_id, record_id, cloud_id "
      + " FROM " + sourceTable + " ;\n";
  private static final String insert = "INSERT INTO "
      + targetTable + " (provider_id, record_id, cloud_id) "
      + "VALUES (?,?,?);\n";

  @Override
  public void migrate(Session session) {
    PreparedStatement selectStatement = session.prepare(select);
    PreparedStatement insertStatement = session.prepare(insert);
    copyTable(session, selectStatement, insertStatement);
  }

  @Override
  public void insert(PreparedStatement insertStatement, Row r, Session session) {
    session.execute(insertStatement.bind(
        r.getString("provider_id"),
        r.getString("record_id"),
        r.getString("cloud_id")
    ));
  }
}
