package eu.europeana.cloud.service.mcs.persistent;

import eu.europeana.cloud.test.CassandraEnvironment;
import eu.europeana.cloud.test.CassandraTestInstance;
import org.junit.jupiter.api.BeforeEach;

public abstract class CassandraTestBase {

  private static final String KEYSPACE_SCHEMA_CQL = "create_cassandra_schema.cql";
  static final String KEYSPACE = "junit_mcs" + CassandraEnvironment.KEYSPACE_SUFFIX;

  public CassandraTestBase() {
    CassandraTestInstance.getInstance(KEYSPACE_SCHEMA_CQL, KEYSPACE);
  }

  @BeforeEach
  public void truncateAll() {
    CassandraTestInstance.truncateAllData(false);
  }
}
