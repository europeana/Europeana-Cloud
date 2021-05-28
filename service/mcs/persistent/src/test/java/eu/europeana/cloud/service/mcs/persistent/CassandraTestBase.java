package eu.europeana.cloud.service.mcs.persistent;

import eu.europeana.cloud.test.CassandraTestInstance;
import org.junit.Before;

public abstract class CassandraTestBase {
    private static final String KEYSPACE_SCHEMA_CQL = "create_cassandra_schema.cql";
    static final String KEYSPACE = "junit_mcs";

    public CassandraTestBase() {
        CassandraTestInstance.getInstance(KEYSPACE_SCHEMA_CQL,KEYSPACE);
    }

    @Before
    public void truncateAll() {
        CassandraTestInstance.truncateAllData(false);
    }
}
