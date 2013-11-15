package eu.europeana.cloud.service.uis.database;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.datastax.driver.core.Cluster;
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
	private String path;
	/**
	 * Initialization of the database connection
	 * 
	 * @param host
	 *            The host to connect to
	 * @param port
	 *            The port to connect to
	 * @param keyspaceName
	 *            The keyspace to connect to
	 */
	public DatabaseService(String host, String port, String keyspaceName, String path) throws IOException {
		this.host = host;
		this.port = port;
		this.keyspaceName = keyspaceName;
		Cluster cluster = new Cluster.Builder().addContactPoints(host).withPort(Integer.parseInt(port)).build();
		session = cluster.connect();
		List<String> cql = FileUtils.readLines(new File(path));
		int i = 0;
		for (String query : cql) {
			if (i < 2) {
				session.execute(String.format(query, keyspaceName));
			} else {
				session.execute(query);
			}
			i++;
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
	public Session getSession() {
		return this.session;
	}

	/**
	 * Expose the contact server IP address
	 * 
	 * @return
	 */
	public String getHost() {
		return host;
	}

	/**
	 * Expose the contact server port
	 * 
	 * @return
	 */
	public String getPort() {
		return port;
	}

	/**
	 * Expose the keyspace
	 * 
	 * @return
	 */
	public String getKeyspaceName() {
		return keyspaceName;
	}

}
