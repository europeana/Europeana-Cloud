package eu.europeana.cloud.test;

import java.io.IOException;

import org.cassandraunit.CQLDataLoader;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

/**
 * Test configuration for Cassandra
 * 
 * TODO: should be merged with {@link CassandraTestRunner}
 */
public abstract class CassandraAATestRunner {

	private static final Logger LOGGER = LoggerFactory
			.getLogger(CassandraAATestRunner.class);

	private static boolean serverRunning = false;

	private static Cluster cluster;

	public static final String KEYSPACE_SCHEMA_CQL = "create_cassandra_schema.cql";

	public static final int PORT = 19142;

	public static final String KEYSPACE = "ecloud_test";

	public static final String CASSANDRA_CONFIG_FILE = "cassandra.yaml";

	/**
	 * Creates a new instance of this class.
	 */
	public CassandraAATestRunner() {
		synchronized (CassandraAATestRunner.class) {
			if (!serverRunning) {
				try {
					LOGGER.info("Starting embedded Cassandra...");
					EmbeddedCassandraServerHelper
							.startEmbeddedCassandra(CASSANDRA_CONFIG_FILE);
					try {
						Thread.sleep(10000l);
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
			LOGGER.info("Embedded Cassandra started successfully...");
		}
	}

	/**
	 * @return The session to connect
	 */
	protected Session getSession() {
		return cluster.connect(KEYSPACE);
	}

	private void initKeyspace() throws IOException {
		initKeyspace(KEYSPACE_SCHEMA_CQL, KEYSPACE);
	}

	private void initKeyspace(final String schema, final String keyspace)
			throws IOException {
		CQLDataLoader dataLoader = new CQLDataLoader(cluster.newSession());
		dataLoader.load(new ClassPathCQLDataSet(schema, keyspace));
	}

	/**
	 * Truncate the tables
	 */
	@After
	public void truncateAll() {
		LOGGER.info("Truncating all tables");
		Session session = getSession();
		ResultSet rs = session
				.execute("SELECT columnfamily_name from system.schema_columnfamilies where keyspace_name='"
						+ KEYSPACE + "';");
		for (Row r : rs.all()) {
			String tableName = r.getString("columnfamily_name");
			LOGGER.info("Truncating table: " + tableName);
			session.execute("TRUNCATE " + tableName);
		}
	}
}
