package eu.europeana.cloud.service.uis.persistent;

import com.datastax.driver.core.Session;
import eu.europeana.cloud.test.CassandraTestInstance;
import org.junit.After;

/**
 * Test configuration for Cassandra
 */
public abstract class CassandraTestBase {

    // config:
    public static final String KEYSPACE_SCHEMA_CQL = "cassandra-uis.cql";
    public static final String KEYSPACE = "uis_test4";

    /**
     * Creates a new instance of this class.
     */
    public CassandraTestBase() {
        CassandraTestInstance.getInstance(KEYSPACE_SCHEMA_CQL, KEYSPACE);
    }

    /**
     * @return The session to connect
     */
    public Session getSession() {
        return CassandraTestInstance.getSession(KEYSPACE);
    }


    /**
     * Truncate the tables
     */
    @After
    public void truncateAll() {
        CassandraTestInstance.truncateAllData(false);
    }
}
