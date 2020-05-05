package eu.europeana.cloud.test;

import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;

public class CassandraTestRunner extends BlockJUnit4ClassRunner {
    private static final String KEYSPACE_SCHEMA_CQL = "create_cassandra_schema.cql";
    private static final String KEYSPACE = "ecloud_test";

    public CassandraTestRunner(Class c)
            throws InitializationError {
        super(c);
        CassandraTestInstance
                .getInstance(KEYSPACE_SCHEMA_CQL, KEYSPACE)
                .initKeyspaceIfNeeded("aas_setup.cql","ecloud_aas");
    }

    @Override
    protected Statement withBefores(FrameworkMethod method, Object target, Statement statement) {
        CassandraTestInstance.truncateAllData(false);
        return super.withBefores(method, target, statement);
    }
}
