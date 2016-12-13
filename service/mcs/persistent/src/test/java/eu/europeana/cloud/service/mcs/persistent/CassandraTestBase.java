package eu.europeana.cloud.service.mcs.persistent;

import eu.europeana.cloud.test.CassandraTestInstance;
import org.junit.After;

public abstract class CassandraTestBase {
    private static final String KEYSPACE_SCHEMA_CQL = "create_cassandra_schema.cql";
    static final String KEYSPACE = "ecloud_test";

    CassandraTestBase() {
        CassandraTestInstance.getInstance(KEYSPACE_SCHEMA_CQL,KEYSPACE);
    }

    @After
    public void truncateAll() {
        CassandraTestInstance.truncateAllData(false);
    }
}
