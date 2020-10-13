package migrations.service.dps.V8;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

public class V8_3__create_altered_processed_records implements JavaMigration {
	@Override
	public void migrate(Session session) throws Exception {
		session.execute(
				"CREATE TABLE processed_records (" +
						"task_id bigint," +
						"record_id varchar," +
						"attempt_number int," +
						"dst_identifier varchar," +
						"topology_name varchar," +
						"state varchar," +
						"start_time     timestamp," +
						"info_text text," +
						"additional_informations text," +
						"PRIMARY KEY(task_id, record_id)" +
						");"
		);
	}
}