package eu.europeana.cloud.service.mcs.persistent.cassandra;

import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

/**
 * Connector to Cassandra cluster.
 */
@Component
public class CassandraConnectionProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraConnectionProvider.class);

    private final Cluster cluster;

    private final Session session;

    private final ConsistencyLevel consistencyLevel = ConsistencyLevel.QUORUM;


    /**
     * Constructor. Use it when your Cassandra cluster does not support authentication.
     * 
     * @param host
     *            cassandra node host
     * @param port
     *            cassandra node cql service port
     * @param keyspaceName
     *            name of keyspace
     */
    public CassandraConnectionProvider(String host, int port, String keyspaceName) {
        cluster = Cluster.builder().addContactPoint(host).withPort(port).build();
        Metadata metadata = cluster.getMetadata();
        LOGGER.info("Connected to cluster: {}", metadata.getClusterName());
        for (Host h : metadata.getAllHosts()) {
            LOGGER.info("Datatacenter: {}; Host: {}; Rack: {}", h.getDatacenter(), h.getAddress(), h.getRack());
        }
        session = cluster.connect(keyspaceName);
    }


    /**
     * Constructor. Use it when your Cassandra cluster does support authentication.
     * 
     * @param host
     *            cassandra node host
     * @param port
     *            cassandra node cql service port
     * @param keyspaceName
     *            name of keyspace
     * @param userName
     *            user name
     * @param password
     *            password
     */
    public CassandraConnectionProvider(String host, int port, String keyspaceName, String userName, String password) {
        cluster = Cluster.builder().addContactPoint(host).withCredentials(userName, password).withPort(port).build();
        Metadata metadata = cluster.getMetadata();
        LOGGER.info("Connected to cluster: {}", metadata.getClusterName());
        for (Host h : metadata.getAllHosts()) {
            LOGGER.info("Datatacenter: {}; Host: {}; Rack: {}", h.getDatacenter(), h.getAddress(), h.getRack());
        }
        session = cluster.connect(keyspaceName);
    }


    @PreDestroy
    private void closeConnections() {
        LOGGER.info("Cluster is shutting down.");
        cluster.close();
    }


    /**
     * Returns session to cassandra cluster.
     * 
     * @return the Session to Cassandra Cluster
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
        return consistencyLevel;
    }
}
