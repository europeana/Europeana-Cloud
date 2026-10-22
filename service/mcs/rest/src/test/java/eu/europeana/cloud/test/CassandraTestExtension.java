package eu.europeana.cloud.test;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public class CassandraTestExtension implements BeforeAllCallback, BeforeEachCallback {

    private static final String KEYSPACE_SCHEMA_CQL = "create_cassandra_schema.cql";

    public static final String JUNIT_MCS_KEYSPACE = "junit_mcs" + CassandraEnvironment.KEYSPACE_SUFFIX;
    public static final String JUNIT_AAS_KEYSPACE = "junit_aas" + CassandraEnvironment.KEYSPACE_SUFFIX;

    @Override
    public void beforeAll(ExtensionContext context) {
        CassandraTestInstance
                .getInstance(KEYSPACE_SCHEMA_CQL, JUNIT_MCS_KEYSPACE)
                .initKeyspaceIfNeeded("aas_setup.cql", JUNIT_AAS_KEYSPACE);
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        CassandraTestInstance.truncateAllData(false);
    }
}
