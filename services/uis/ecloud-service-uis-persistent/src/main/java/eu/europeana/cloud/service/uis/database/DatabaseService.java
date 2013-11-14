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

	private Session session;
	private String host;
	private String port;
	private String keyspaceName;

	private final static String CLOUD_ID = "CREATE TABLE Cloud_Id(cloud_id varchar, provider_id varchar, record_id varchar, deleted boolean, "
			+ "PRIMARY KEY (cloud_id, provider_id,record_id));";
	private final static String CLOUD_ID_SECONDARY_INDEX = "CREATE INDEX deleted_records ON Cloud_Id(deleted);";

	private final static String PROVIDER_RECORD_ID = "CREATE TABLE Provider_Record_Id(provider_id varchar, record_id varchar, cloud_id varchar,"
			+ " deleted boolean, PRIMARY KEY (provider_id,record_id));";
	
	private final static String PROVIDER_RECORD_ID_SECONDARY_INDEX = "CREATE INDEX record_deleted on Provider_Record_Id(deleted);";
	
	/**
	 * Initialization of the database connection
	 * @param host The host to connect to
	 * @param port The port to connect to
	 * @param keyspaceName The keyspace to connect to
	 */
	public DatabaseService(String host, String port, String keyspaceName) {
		this.host = host;
		this.port = port;
		this.keyspaceName = keyspaceName;
		Cluster cluster = new Cluster.Builder().addContactPoints(host).withPort(Integer.parseInt(port)).build();
		session = cluster.connect();
		if (session.getCluster().getMetadata().getKeyspace(keyspaceName) == null) {

			session.execute("CREATE KEYSPACE " + keyspaceName + " WITH replication "
					+ "= {'class':'SimpleStrategy', 'replication_factor':1};");
			session.execute("USE "+keyspaceName +";");
			session.execute(CLOUD_ID);
			session.execute(CLOUD_ID_SECONDARY_INDEX);
			session.execute(PROVIDER_RECORD_ID);
			session.execute(PROVIDER_RECORD_ID_SECONDARY_INDEX);
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
