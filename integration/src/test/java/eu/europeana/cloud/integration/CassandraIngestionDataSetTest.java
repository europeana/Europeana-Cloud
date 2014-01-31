/* IngestionDataSetTest.java - created on Jan 7, 2014, Copyright (c) 2011 The European Library, all rights reserved */
package eu.europeana.cloud.integration;

import java.io.IOException;

import org.cassandraunit.CQLDataLoader;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * This class test integration concerning operation on dataset basis. It tests the whole workflow
 * concerning ingestion of whole datasets as well as reading them.
 * 
 * @author Markus Muhr (markus.muhr@kb.nl)
 * @since Jan 7, 2014
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = { "classpath:/persistentServicesTestContext.xml" })
public class CassandraIngestionDataSetTest extends IngestionDataSetTest {
    private static final Logger LOGGER                = LoggerFactory.getLogger(CassandraIngestionDataSetTest.class);

    private static boolean      serverRunning         = false;

    private static Cluster      cluster;

    // config:
    private static final int    PORT                  = 19142;

    private static final String CASSANDRA_CONFIG_FILE = "cassandra.yaml";

    private static final String KEYSPACE              = "integration_test";

    private static final String KEYSPACE_SCHEMA_CQL   = "cassandra.cql";

    /**
     * Creates a new instance of this class.
     */
    public CassandraIngestionDataSetTest() {
        synchronized (CassandraIngestionDataSetTest.class) {
            if (!serverRunning) {
                try {
                    LOGGER.info("Starting embedded Cassandra...");
                    EmbeddedCassandraServerHelper.startEmbeddedCassandra(CASSANDRA_CONFIG_FILE);
                } catch (Exception e) {
                    LOGGER.error("Cannot start embedded Cassandra!", e);
                    throw new RuntimeException("Cannot start embedded Cassandra!", e);
                }
                cluster = Cluster.builder().addContactPoint("localhost").withPort(PORT).build();
                try {
                    LOGGER.info("Initializing keyspace...");
                    initKeyspace();
                } catch (IOException e) {
                    LOGGER.error("Cannot initialize keyspace!", e);
                    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
                    throw new RuntimeException("Cannot initialize keyspace!", e);
                }
                serverRunning = true;
            }
        }
    }

    /**
     * @return session
     */
    protected Session getSession() {
        return cluster.connect(KEYSPACE);
    }

    private void initKeyspace() throws IOException {
        CQLDataLoader dataLoader = new CQLDataLoader("localhost", PORT);
        dataLoader.load(new ClassPathCQLDataSet(KEYSPACE_SCHEMA_CQL, KEYSPACE));
    }

    /**
     * Truncates all tables.
     */
    @After
    public void truncateAll() {
        LOGGER.info("Truncating all tables");
        Session session = getSession();
        ResultSet rs = session.execute("SELECT columnfamily_name from system.schema_columnfamilies where keyspace_name='" +
                                       KEYSPACE + "';");
        for (Row r : rs.all()) {
            String tableName = r.getString("columnfamily_name");
            LOGGER.info("Truncating table: " + tableName);
            session.execute("TRUNCATE " + tableName);
        }
    }
}
