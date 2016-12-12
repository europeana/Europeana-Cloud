package migrations.service.mcs.V4;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.Session;

public class V5_1__create_latest_revisions_for_dataset_assignment_table implements JavaMigration {
	@Override
	public void migrate(Session session) {
		session.execute("CREATE TABLE latest_revisions_for_dataset_assignment (\n" +
				"provider_id varchar,\n" +
				"dataset_id varchar,\n" +
				"representation_id varchar,\n" +
				"revision_name varchar,\n" +
				"revision_provider_id varchar,\n" +
				"revision_timestamp timestamp,\n" +
				"cloud_id varchar,\n" +
				"version_id timeuuid,\n" +
				"acceptance boolean,\n"
				"published boolean,\n"
				"mark_deleted boolean,\n"
				"PRIMARY KEY ((provider_id, dataset_id, representation_id, revision_name, revision_provider_id), cloud_id)\n" +
				")WITH comment='Retrieve assignment information based on a known cloud_id and known clustering keys.';\n");

		session.execute("CREATE INDEX latest_revisions_for_dataset_assignment_acceptance ON latest_revisions_for_dataset_assignment (acceptance);\n");
		session.execute("CREATE INDEX latest_revisions_for_dataset_assignment_published ON latest_revisions_for_dataset_assignment (published);\n");
		session.execute("CREATE INDEX latest_revisions_for_dataset_assignment_deleted ON latest_revisions_for_dataset_assignment (deleted);\n");
	}
}