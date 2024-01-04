package eu.europeana.cloud.cassandra;

import com.datastax.driver.core.AtomicMonotonicTimestampGenerator;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import javax.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;


/**
 * Connector to Cassandra cluster.
 */
@Component
public class CassandraConnectionProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraConnectionProvider.class);

  private static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM;

  private final Cluster cluster;

  private final Session session;

  private final String hosts;

  private final String port;

  private final String keyspaceName;

  /**
   * Constructor. Use it when your Cassandra cluster does not support authentication.
   *
   * @param hosts cassandra node hosts, comma separated
   * @param port cassandra node cql service port
   * @param keyspaceName name of keyspace
   */
  public CassandraConnectionProvider(String hosts, int port, String keyspaceName) {
    this.hosts = hosts;
    this.port = String.valueOf(port);
    this.keyspaceName = keyspaceName;

    String[] contactPoints = hosts.split(",");
    cluster = getClusterBuilder(port, contactPoints)
        .build();
    init();
    session = cluster.connect(keyspaceName);
  }


  /**
   * Constructor. Use it when your Cassandra cluster does support authentication.
   *
   * @param hosts cassandra node hosts, comma separated
   * @param port cassandra node cql service port
   * @param keyspaceName name of keyspace
   * @param userName user name
   * @param password password
   */
  public CassandraConnectionProvider(String hosts, int port, String keyspaceName, String userName, String password) {
    this.hosts = hosts;
    this.port = String.valueOf(port);
    this.keyspaceName = keyspaceName;

    String[] contactPoints = hosts.split(",");
    cluster = getClusterBuilder(port, contactPoints)
        .withCredentials(userName, password).build();
    init();
    session = cluster.connect(keyspaceName);
  }

  /**
   * Common initializer.
   */
  private void init() {
    Metadata metadata = cluster.getMetadata();
    LOGGER.info("Connected to cluster: {}", metadata.getClusterName());
    for (Host h : metadata.getAllHosts()) {
      LOGGER.info("Data center: {}; Hosts: {}; Rack: {}", h.getDatacenter(), h.getBroadcastAddress(), h.getRack());
    }
  }

  /**
   * Obtain common cassandra Cluster Builder.
   *
   * @param port port of endpoints
   * @param contactPoints list of cassandra ip addresses
   * @return {@link com.datastax.driver.core.Cluster.Builder}
   */
  private Cluster.Builder getClusterBuilder(int port, String[] contactPoints) {
    return Cluster.builder().addContactPoints(contactPoints).withPort(port)
                  .withProtocolVersion(ProtocolVersion.V3)
                  .withQueryOptions(new QueryOptions()
                      .setConsistencyLevel(DEFAULT_CONSISTENCY_LEVEL))
                  .withTimestampGenerator(new AtomicMonotonicTimestampGenerator());
  }


  /**
   * Closes connection to the cluster specified in the class instance
   */
  @PreDestroy
  public void closeConnections() {
    LOGGER.info("Cluster is shutting down.");
    cluster.close();
  }

  /**
   * Expose a singleton instance connection to a database on the requested host and keyspace
   *
   * @return A session to a Cassandra connection
   */
  public Session getSession() {
    return session;
  }


  /**
   * Returns the default consistency level;
   *
   * @return the consistencyLevel
   */
  public ConsistencyLevel getConsistencyLevel() {
    return DEFAULT_CONSISTENCY_LEVEL;
  }


  /**
   * Expose the contact server IP address
   *
   * @return The host name
   */
  public String getHosts() {
    return hosts;
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

  public Metadata getMetadata() {
    return cluster.getMetadata();
  }
}
