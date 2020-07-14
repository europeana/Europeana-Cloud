package eu.europeana.cloud.test;

import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;

public class CassandraTestRunner extends BlockJUnit4ClassRunner {

    public static final String USE_STANDALONE_CASSANDRA_SYSTEM_PROPERTY_NAME = "use.standalone.cassandra.for.test";

    public static final boolean USE_EMBEEDED_CASSANDRA=!Boolean.getBoolean(USE_STANDALONE_CASSANDRA_SYSTEM_PROPERTY_NAME);

    public static final int EMBEEDED_CASSANDRA_PORT = USE_EMBEEDED_CASSANDRA?9142:9042;

    private static final String KEYSPACE_SCHEMA_CQL = "create_cassandra_schema.cql";

    public static final String JUNIT_MCS_KEYSPACE = "junit_mcs";

    public static final String JUNIT_AAS_KEYSPACE = "junit_aas";

    public CassandraTestRunner(Class c)
            throws InitializationError {
        super(c);
        if(USE_EMBEEDED_CASSANDRA) {
            CassandraTestInstance
                    .getInstance(KEYSPACE_SCHEMA_CQL, JUNIT_MCS_KEYSPACE)
                    .initKeyspaceIfNeeded("aas_setup.cql", JUNIT_AAS_KEYSPACE);
        }
    }

    @Override
    protected Statement withBefores(FrameworkMethod method, Object target, Statement statement) {
        if(USE_EMBEEDED_CASSANDRA) {
            CassandraTestInstance.truncateAllData(false);
        }
        return super.withBefores(method, target, statement);
    }
}
