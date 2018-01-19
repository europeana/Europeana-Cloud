package eu.europeana.cloud.service.dps.storm.topologies.validation.topology.helper;

import com.datastax.driver.core.Session;
import eu.europeana.cloud.test.CassandraTestInstance;
import org.junit.Before;

public abstract class CassandraTestBase {
    protected static final String KEYSPACE = "ecloud_test";
    private static final String KEYSPACE_SCHEMA_CQL = "create_test_dps_schema.cql";
    public static final int PORT = 19142;
    public static final String HOST = "localhost";


    public CassandraTestBase() {
        CassandraTestInstance.getInstance(KEYSPACE_SCHEMA_CQL, KEYSPACE);
    }

    protected Session getSession() {
        return CassandraTestInstance.getSession(KEYSPACE);
    }

    @Before
    public void truncateAll() {
        CassandraTestInstance.truncateAllData(false);
    }
}
