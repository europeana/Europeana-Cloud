package eu.europeana.cloud.test;

import org.junit.After;

/**
 * Test configuration for Cassandra
 */
public abstract class CassandraAATestRunner {

	private final CassandraTestInstance cassandraTestInstance;
	public static final String KEYSPACE_SCHEMA_CQL = "create_cassandra_schema.cql";
	public static final String KEYSPACE = "ecloud_aas_tests";

	public CassandraAATestRunner() {
		cassandraTestInstance = CassandraTestInstance.getInstance(KEYSPACE_SCHEMA_CQL,KEYSPACE);
	}

	/**
	 * Truncate the tables
	 */
	@After
	public void tearDown() {
		cassandraTestInstance.truncateAllData();
	}


}
