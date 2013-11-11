package eu.europeana.cloud.service.uis.database;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * Database service providing the connection to the database
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * 
 */
public class DatabaseService {

	private Cluster cluster;
	private Session session;
	private String host;
	private String port;
	private String keyspaceName;
	
	public DatabaseService(String host, String port, String keyspaceName) {
		this.host = host;
		this.port=port;
		this.keyspaceName = keyspaceName;
		cluster = new Cluster.Builder().addContactPoints(host).build();
		session = cluster.connect(keyspaceName);
	}

	/**
	 * Expose a singleton instance connection to a database on the requested
	 * host and keyspace
	 * 
	 * @param host
	 *            The host to connect to
	 * @param keyspaceName
	 *            The keyspace to connect to
	 * @return A session to a Cassandra connection
	 */
	public Session getSession() {
		return this.session;
	}
	
	/**
	 * Expose the contact server IP address
	 * @return
	 */
	public String getHost() {
		return host;
	}

	/**
	 * Expose the contact server port
	 * @return
	 */
	public String getPort() {
		return port;
	}

	/**
	 * Expose the keyspace
	 * @return
	 */
	public String getKeyspaceName() {
		return keyspaceName;
	}

}
