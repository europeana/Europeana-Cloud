package eu.europeana.cloud.service.commons.cassandra;

import com.datastax.driver.core.Session;
import eu.europeana.cloud.test.CassandraTestInstance;
import org.junit.Before;

public class CassandraTestBase {
    /**
     * CassandraTestBase KEYSPACE_SCHEMA_CQL
     */
    // config:
    private static final String KEYSPACE_SCHEMA_CQL = "tests_schema.cql";
    /**
     * CassandraTestBase KEYSPACE
     */
    private static final String KEYSPACE = "test_keyspace";

    /**
     * Creates a new instance of this class.
     */
    public CassandraTestBase() {
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
