package eu.europeana.cloud.service.uis.security;

import com.datastax.driver.core.Session;
import eu.europeana.cloud.test.CassandraTestInstance;
import org.junit.Before;

/**
 * Test configuration for Cassandra
 */
public abstract class CassandraTestBase {

  /**
   * CassandraTestBase KEYSPACE_SCHEMA_CQL
   */
  // config:
  private static final String KEYSPACE_SCHEMA_CQL = "cassandra-aas.cql";
  /**
   * CassandraTestBase KEYSPACE
   */
  private static final String KEYSPACE = "aas_test";

  /**
   * Creates a new instance of this class.
   */
  CassandraTestBase() {
    CassandraTestInstance.getInstance(KEYSPACE_SCHEMA_CQL, KEYSPACE);
  }

  /**
   * @return session
   */
  protected Session getSession() {
    return CassandraTestInstance.getSession(KEYSPACE);
  }

  /**
   * Truncates all tables.
   */
  @Before
  public void truncateAll() {
    CassandraTestInstance.truncateAllData(false);
  }
}

