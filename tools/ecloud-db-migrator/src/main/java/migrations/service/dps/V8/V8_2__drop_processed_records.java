package migrations.service.dps.V8;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

public class V8_2__drop_processed_records implements JavaMigration {
	@Override
	public void migrate(Session session) throws Exception {
		session.execute(
				"DROP TABLE processed_records;"
		);
	}
}
