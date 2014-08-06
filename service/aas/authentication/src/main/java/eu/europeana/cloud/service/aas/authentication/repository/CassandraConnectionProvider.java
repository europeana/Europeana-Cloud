package eu.europeana.cloud.service.aas.authentication.repository;

import java.io.File;
import java.io.IOException;
import java.util.List;

import javax.annotation.PreDestroy;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;

/**
 * Database service providing the connection to the database
 * 
 * @author Yorgos.Mamakis@ kb.nl
 */
public class CassandraConnectionProvider {

	private Session session;
	private String host;
	private String port;

	private String keyspaceName;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(CassandraConnectionProvider.class);

	/**
	 * Initialization of the database connection.
	 * 
	 * @param host The host to connect to
	 * @param port The port to connect to
	 * @param keyspaceName The keyspace to connect to
	 * 
	 * @param username The username
	 * @param password The password
	 * 
	 * @param create If true a user + password table will be created in the database.
	 * @throws IOException
	 */
	public CassandraConnectionProvider(final String host, final String port, final String keyspaceName, final String username, final String password,
			final boolean createTables) throws IOException {

        LOGGER.info("DatabaseService starting... host='{}', port='{}', keyspaceName='{}', create='{}',username='{}'",
        		host, port, keyspaceName, createTables, username);
        
		this.host = host;
		this.port = port;
		this.keyspaceName = keyspaceName;
		
		final Cluster cluster = new Cluster.Builder().addContactPoints(host).withPort(Integer.parseInt(port))
				.withCredentials(username, password).build();
		session = cluster.connect(keyspaceName);
		
		if (createTables) {
	        LOGGER.info("DatabaseService creating tables...");
			session = cluster.connect();
			List<String> cql = FileUtils.readLines(new File(getClass().getResource("/cassandra-aas.cql").getPath()));
			int i = 0;
			for (String query : cql) {
				if (i < 2) {
					session.execute(String.format(query, keyspaceName));
				} else {
					session.execute(query);
				}
				i++;
			}
	        LOGGER.info("DatabaseService tables created successfully.");
		}
        LOGGER.info("DatabaseService started successfully.");
	}

	/**
	 * Expose a singleton instance connection to a database on the requested
	 * host and keyspace
	 * 
	 * @return A session to a Cassandra connection
	 */
	public Session getSession() {
		return this.session;
	}

	/**
	 * Expose the contact server IP address
	 * 
	 * @return The host name
	 */
	public String getHost() {
		return host;
	}

	/**
	 * Expose the contact server port
	 * 
	 * @return The host port
	 */
	public String getPort() {
		return port;
	}

	/**
	 * Expose the keyspace
	 * 
	 * @return The keyspace name
	 */
	public String getKeyspaceName() {
		return keyspaceName;
	}

	/**
	 * Retrieve the consistency level of the queries
	 * 
	 * @return QUORUM consistency level
	 */
	public ConsistencyLevel getConsistencyLevel() {
		return ConsistencyLevel.QUORUM;
	}

	@PreDestroy
	    private void closeConnections() {
	        LOGGER.info("closeConnections() Cluster is shutting down.");
	        session.getCluster().close();
	        LOGGER.info("closeConnections() Cluster shut down successfully.");
	    }
}
