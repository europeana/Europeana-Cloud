package eu.europeana.cloud.service.mcs.persistent;

import org.springframework.stereotype.Service;

import com.datastax.driver.core.Cluster;

/**
 * CassandraConnectionProvider
 */
@Service
public class CassandraConnectionProvider {

    private final String cassandraHost;

    private final int cassandraPort;

    private final String keyspaceName;


    public CassandraConnectionProvider(String cassandraHost, int cassandraPort, String keyspaceName) {
        this.cassandraHost = cassandraHost;
        this.cassandraPort = cassandraPort;
        this.keyspaceName = keyspaceName;
    }
    
    

}
