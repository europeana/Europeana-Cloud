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
import org.junit.runner.Description;
import org.junit.runner.notification.RunListener;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;

public class CassandraTestRunner extends BlockJUnit4ClassRunner {

	private static final Logger LOGGER = LoggerFactory.getLogger(CassandraTestRunner.class);

	private static boolean serverRunning = false;

	private static Cluster cluster;

	// config:
	public static final String KEYSPACE_SCHEMA_CQL = "create_cassandra_schema.cql";

	public static final int PORT = 9142;

	public static final String KEYSPACE = "ecloud_test";

	public static final String CASSANDRA_CONFIG_FILE = EmbeddedCassandraServerHelper.DEFAULT_CASSANDRA_YML_FILE;


	public CassandraTestRunner(Class c) throws InitializationError {
		super(c);
		synchronized (CassandraTestRunner.class) {
			if (!serverRunning) {
				try {
					LOGGER.info("Starting embedded Cassandra");
					EmbeddedCassandraServerHelper.startEmbeddedCassandra(CASSANDRA_CONFIG_FILE);
					cluster = Cluster.builder().addContactPoint("localhost").withPort(PORT).build();
					initKeyspace();
					serverRunning = true;
				} catch (Exception e) {
					LOGGER.error("Cannot start embedded Cassandra!", e);
					EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
					throw new RuntimeException("Cannot start embedded Cassandra!", e);
				}
			}
		}
	}


	protected Session getSession() {
		return cluster.connect(KEYSPACE);
	}


	private void initKeyspace()
			throws IOException {
		CQLDataLoader dataLoader = new CQLDataLoader("localhost", PORT);
		dataLoader.load(new ClassPathCQLDataSet(KEYSPACE_SCHEMA_CQL, KEYSPACE));
	}


	public void truncateAll() {
		LOGGER.info("Truncating all tables");
		Session session = getSession();
		ResultSet rs = session.
				execute("SELECT columnfamily_name from system.schema_columnfamilies where keyspace_name='" + KEYSPACE + "';");
		for (Row r : rs.all()) {
			String tableName = r.getString("columnfamily_name");
			LOGGER.info("Truncating table: " + tableName);
			session.execute("TRUNCATE " + tableName);
		}
	}



	@Override
	public void run(RunNotifier rn) {
		super.run(rn);
		rn.addListener(new RunListener() {

			@Override
			public void testFinished(Description description)
					throws Exception {
				truncateAll();
			}

		});
	}
}
