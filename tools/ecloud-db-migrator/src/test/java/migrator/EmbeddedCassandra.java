package migrator;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.cassandraunit.CQLDataLoader;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmbeddedCassandra extends ExternalResource {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(EmbeddedCassandra.class);

    private static boolean serverRunning = false;

    private static Cluster cluster;

    /**
     * CassandraTestBase KEYSPACE_SCHEMA_CQL
     */
    // config:
    public static final String KEYSPACE_SCHEMA_CQL = "create_cassandra_schema.cql";

    /**
     * CassandraTestBase PORT
     */
    public static final int PORT = 9142;

    /**
     * CassandraTestBase KEYSPACE
     */
    public static final String KEYSPACE = "test_keyspace";

    /**
     * CassandraTestBase CASSANDRA_CONFIG_FILE
     */
    public static final String CASSANDRA_CONFIG_FILE = "cassandra_config.yaml";
    private static final long TIME_FOR_BOOT_UP_CASSANDRA = 10000l;

    /**
     * Creates a new instance of this class.
     */
    public EmbeddedCassandra() {
        synchronized (EmbeddedCassandra.class) {
            if (!serverRunning) {
                try {
                    LOGGER.info("Starting embedded Cassandra...");
                    EmbeddedCassandraServerHelper
                            .startEmbeddedCassandra(CASSANDRA_CONFIG_FILE);
                    try {
                        Thread.sleep(TIME_FOR_BOOT_UP_CASSANDRA);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(
                                "Caused by InterruptedException", e);
                    }
                } catch (Exception e) {
                    LOGGER.error("Cannot start embedded Cassandra!", e);
                    throw new RuntimeException(
                            "Cannot start embedded Cassandra!", e);
                }
                cluster = Cluster.builder().addContactPoint("localhost")
                        .withPort(PORT).build();
                serverRunning = true;
            }
        }
    }

    @Override
    protected void before() throws Throwable {
        crateKeyspace();
    }

    @Override
    protected void after() {
        dropAllKeyspaces();
    }

    protected Session getSession() {
        return cluster.connect(KEYSPACE);
    }

    private void dropAllKeyspaces() {
        LOGGER.info("Drop all keyspaces...");
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }

    private void crateKeyspace() {
        CQLDataLoader dataLoader = new CQLDataLoader(cluster.newSession());
        dataLoader.load(new ClassPathCQLDataSet(KEYSPACE_SCHEMA_CQL, KEYSPACE));
    }

}