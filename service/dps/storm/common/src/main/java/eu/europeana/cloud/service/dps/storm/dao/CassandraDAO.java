package eu.europeana.cloud.service.dps.storm.dao;

import com.datastax.driver.core.PreparedStatement;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;

/**
 * @author krystian.
 */
public abstract class CassandraDAO {
    protected String hostList;
    protected String keyspaceName;
    protected String port;
    protected CassandraConnectionProvider dbService;

    protected abstract void prepareStatements();

    protected CassandraDAO(){
    }

    public CassandraDAO(CassandraConnectionProvider dbService) {
        this.keyspaceName = dbService.getKeyspaceName();
        this.dbService = dbService;
        this.port = dbService.getPort();
        this.hostList = dbService.getHosts();
        prepareStatements();
    }

    public String getHostList() {
        return hostList;
    }

    public String getKeyspace() {
        return keyspaceName;
    }

    public String getPort() {
        return this.port;
    }

    protected PreparedStatement prepare(String query) {
        PreparedStatement statement = dbService.getSession().prepare(query);
        return statement;
    }
}
