package eu.europeana.cloud.test;

import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;

public class CassandraTestRunner extends BlockJUnit4ClassRunner {

  private static final String KEYSPACE_SCHEMA_CQL = "create_cassandra_schema.cql";

  public static final String JUNIT_MCS_KEYSPACE = "junit_mcs";

  public static final String JUNIT_AAS_KEYSPACE = "junit_aas";

  public CassandraTestRunner(Class c)
      throws InitializationError {
    super(c);
    CassandraTestInstance
        .getInstance(KEYSPACE_SCHEMA_CQL, JUNIT_MCS_KEYSPACE)
        .initKeyspaceIfNeeded("aas_setup.cql", JUNIT_AAS_KEYSPACE);
  }

  @Override
  protected Statement withBefores(FrameworkMethod method, Object target, Statement statement) {
    CassandraTestInstance.truncateAllData(false);
    return super.withBefores(method, target, statement);
  }
}
