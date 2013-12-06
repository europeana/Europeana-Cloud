package eu.europeana.cloud.service.mcs.persistent;

import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.datastax.driver.core.*;

/**
 * Connector to Cassandra cluster.
 */
@Component
public class CassandraConnectionProvider {

    private final static Logger log = LoggerFactory.getLogger(CassandraConnectionProvider.class);

    private final Cluster cluster;

    private final Session session;


    /**
     * Constructor.
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
        log.info("Connected to cluster: {}", metadata.getClusterName());
        for (Host h : metadata.getAllHosts()) {
            log.info("Datatacenter: {}; Host: {}; Rack: {}", h.getDatacenter(), h.getAddress(), h.getRack());
        }
        session = cluster.connect(keyspaceName);
    }


    @PreDestroy
    public void closeConnections() {
        log.info("Cluster is shutting down.");
        ShutdownFuture shutdownFuture = cluster.shutdown();
    }


    /**
     * Returns session to cassandra cluster.
     * 
     * @return
     */
    public Session getSession() {
        return session;
    }
}
