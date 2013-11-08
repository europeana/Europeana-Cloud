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
	private static Session session;

	private DatabaseService(String host, String keyspaceName) {
		if (cluster == null && cluster.getMetadata().getAllHosts().contains(host)) {
			cluster = new Cluster.Builder().addContactPoints(host).build();

		}
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
	public static Session getSession(String host, String keyspaceName) {
		new DatabaseService(host, keyspaceName);
		return session;
	}

}
