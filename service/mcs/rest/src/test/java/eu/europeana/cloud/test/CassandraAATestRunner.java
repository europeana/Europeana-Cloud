package eu.europeana.cloud.test;

import org.junit.Before;

/**
 * Test configuration for Cassandra
 */
public abstract class CassandraAATestRunner {

	private static final String KEYSPACE_SCHEMA_CQL = "create_cassandra_schema.cql";
	private static final String KEYSPACE = "ecloud_aas_tests";

	CassandraAATestRunner() {
		CassandraTestInstance.getInstance(KEYSPACE_SCHEMA_CQL,KEYSPACE);
	}

	/**
	 * Truncate the tables
	 */
	@Before
	public void init() {
		CassandraTestInstance.truncateAllData(false);
	}


}
