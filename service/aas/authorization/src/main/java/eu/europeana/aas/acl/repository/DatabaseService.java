package eu.europeana.aas.acl.repository;

import java.io.IOException;

import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;

/**
 * Database service providing the connection to the database
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * 
 */
public class DatabaseService {

	private Session session;
	private String host;
	private String port;

	private String keyspaceName;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseService.class);

	/**
	 * Initialization of the database connection
	 * 
	 * @param host The host to connect to
	 * @param port The port to connect to
	 * @param keyspaceName  The keyspace to connect to
	 * 
	 * @param username The username
	 * @param password The password
	 * @throws IOException
	 */
	public DatabaseService(String host, String port, String keyspaceName, String username, String password) throws IOException {

        LOGGER.info("DatabaseService starting... host='{}', port='{}', keyspaceName='{}', create='{}',username='{}'",
        		host, port, keyspaceName, username);
		this.host = host;
		this.port = port;
		this.keyspaceName = keyspaceName;
		Cluster cluster = new Cluster.Builder().addContactPoints(host).withPort(Integer.parseInt(port))
				.withCredentials(username, password).build();
		session = cluster.connect(keyspaceName);
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
