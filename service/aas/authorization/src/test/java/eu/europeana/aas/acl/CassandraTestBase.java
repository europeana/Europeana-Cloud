package eu.europeana.aas.acl;

import com.datastax.driver.core.Session;
import eu.europeana.cloud.test.CassandraTestInstance;
import org.junit.After;

/**
 * Test configuration for Cassandra
 */
public abstract class CassandraTestBase {
    /**
     * CassandraTestBase KEYSPACE_SCHEMA_CQL
     */
    // config:
    public static final String KEYSPACE_SCHEMA_CQL = "cassandra-aas.cql";
    /**
     * CassandraTestBase KEYSPACE
     */
    public static final String KEYSPACE = "aas_test";

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