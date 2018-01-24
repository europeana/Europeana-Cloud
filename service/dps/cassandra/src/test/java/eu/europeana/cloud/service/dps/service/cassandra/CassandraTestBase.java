package eu.europeana.cloud.service.dps.service.cassandra;

import eu.europeana.cloud.test.CassandraTestInstance;
import org.junit.Before;

public abstract class CassandraTestBase {
    protected static final String KEYSPACE = "ecloud_test";
    private static final String KEYSPACE_SCHEMA_CQL = "create_test_dps_schema.cql";
    public static final int PORT = 19142;
    public static final String HOST = "localhost";

    CassandraTestBase() {
        CassandraTestInstance.getInstance(KEYSPACE_SCHEMA_CQL,KEYSPACE);
    }

    @Before
    public void truncateAll() {
        CassandraTestInstance.truncateAllData(false);
    }
}
