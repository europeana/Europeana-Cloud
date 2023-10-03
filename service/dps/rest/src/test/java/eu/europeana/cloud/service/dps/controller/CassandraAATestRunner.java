package eu.europeana.cloud.service.dps.controller;

import com.datastax.driver.core.Session;
import eu.europeana.cloud.test.CassandraTestInstance;
import org.junit.Before;

/**
 * Test configuration for Cassandra
 */
public abstract class CassandraAATestRunner {

  private final static String KEYSPACE_SCHEMA_CQL = "cassandra-aas.cql";
  private final static String KEYSPACE = "ecloud_aas_tests";


  /**
   * Creates a new instance of this class.
   */
  public CassandraAATestRunner() {
    CassandraTestInstance.getInstance(KEYSPACE_SCHEMA_CQL, KEYSPACE);
  }

  /**
   * @return The session to connect
   */
  protected Session getSession() {
    return CassandraTestInstance.getSession(KEYSPACE);
  }

  /**
   * Truncate the tables
   */
  @Before
  public void truncateAll() {
    CassandraTestInstance.truncateAllData(false);
  }
}
