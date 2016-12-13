package eu.europeana.cloud.service.aas.authentication;

import com.datastax.driver.core.Session;
import eu.europeana.cloud.test.CassandraTestInstance;
import org.junit.After;

/**
 * Test configuration for Cassandra
 */
public abstract class CassandraTestBase {
    /**
     * CassandraTestBase KEYSPACE
     */
    public static final String KEYSPACE = "aas_test";
    /**
     * CassandraTestBase KEYSPACE_SCHEMA_CQL
     */
    // config:
    private static final String KEYSPACE_SCHEMA_CQL = "cassandra-aas.cql";

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
    @After
    public void truncateAll() {
        CassandraTestInstance.truncateAllData(false);
    }
}