package eu.europeana.cloud.service.uis.database;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * Database service providing the connection to the database
 * @author ecloud
 *
 */
public class DatabaseService {

	private Cluster cluster;
	private static Session session;

	private DatabaseService(String host, String keyspaceName) {
		if (cluster == null) {
			cluster = new Cluster.Builder().addContactPoints(host).build();
			
		}
		session = cluster.connect(keyspaceName);
	}

	public static Session getSession(String host, String keyspaceName) {
		new DatabaseService(host, keyspaceName);
		return session;
	}
	
}
