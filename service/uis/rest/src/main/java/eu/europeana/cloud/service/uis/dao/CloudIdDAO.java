package eu.europeana.cloud.service.uis.dao;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.annotation.Retryable;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.service.uis.exception.DatabaseConnectionException;
import eu.europeana.cloud.service.uis.status.IdentifierErrorTemplate;

import java.util.ArrayList;
import java.util.List;

/**
 * DAO providing access to operations on CloudId in the database
 */
@Retryable
public class CloudIdDAO {

    private final String hostList;
    private final String keyspaceName;
    private final String port;
    private final CassandraConnectionProvider dbService;

    private PreparedStatement insertStatement;
    private PreparedStatement searchStatementNonActive;
    private PreparedStatement deleteStatement;

    /**
     * The DAO for Cloud identifiers
     *
     * @param dbService The service exposing the connection and session
     */
    public CloudIdDAO(CassandraConnectionProvider dbService) {
        this.dbService = dbService;
        this.hostList = dbService.getHosts();
        this.port = dbService.getPort();
        this.keyspaceName = dbService.getKeyspaceName();
        prepareStatements();
    }

    private void prepareStatements() {
        insertStatement = dbService.getSession().prepare("insert into cloud_id(cloud_id,provider_id,record_id) values(?,?,?)");

        searchStatementNonActive = dbService.getSession().prepare("select * from cloud_id where cloud_id=?");

        deleteStatement = dbService.getSession().prepare("delete from cloud_id where cloud_id=? and provider_id=? and record_id=?");
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

    public List<CloudId> searchById(String cloudId) throws DatabaseConnectionException {
        try {
            ResultSet rs = dbService.getSession().execute(searchStatementNonActive.bind(cloudId));
            List<CloudId> cloudIds = new ArrayList<>();
            if (!rs.iterator().hasNext()) {
                return cloudIds;
            }

            for (Row row : rs.all()) {
                CloudId cId = createFrom(row);
                cloudIds.add(cId);
            }

            return cloudIds;
        } catch (NoHostAvailableException e) {
            throw new DatabaseConnectionException(new IdentifierErrorInfo(
                    IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getHttpCode(),
                    IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(hostList, port, e.getMessage())));
        }
    }

    /**
     * Search for all the Cloud Identifiers regardless if they are deleted or
     * not
     *
     * @param cloudId The cloudId to search on
     * @return A list of cloudIds
     */
    public List<CloudId> searchAll(String cloudId) {
        ResultSet rs = dbService.getSession().execute(searchStatementNonActive.bind(cloudId));
        List<Row> results = rs.all();
        List<CloudId> cloudIds = new ArrayList<>();
        for (Row row : results) {
            CloudId cId = createFrom(row);
            cloudIds.add(cId);
        }
        return cloudIds;
    }

    public CloudId insert(String cloudId, String providerId, String recordId) throws DatabaseConnectionException {
        try {
            dbService.getSession().execute(bindInsertStatement(cloudId, providerId, recordId));
        } catch (NoHostAvailableException e) {
            throw new DatabaseConnectionException(new IdentifierErrorInfo(
                    IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getHttpCode(),
                    IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(hostList, port, e.getMessage())));
        }

        return new CloudId(cloudId, new LocalId(providerId, recordId));
    }

    public BoundStatement bindInsertStatement(String cloudId, String providerId, String recordId) {
        return insertStatement.bind(cloudId, providerId, recordId);
    }

    public void delete(String cloudId, String providerId, String recordId) throws DatabaseConnectionException {
        try {
            dbService.getSession().execute(deleteStatement.bind(cloudId, providerId, recordId));
        } catch (NoHostAvailableException e) {
            throw new DatabaseConnectionException(
                    new IdentifierErrorInfo(
                            IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getHttpCode(),
                            IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(hostList, port,e.getMessage()))
            );
        }
    }

    private CloudId createFrom(Row row){
        return CloudId.builder()
                .id(row.getString("cloud_id"))
                .localId(LocalId.builder()
                        .providerId(row.getString("provider_Id"))
                        .recordId(row.getString("record_Id"))
                        .build())
                .build();
    }
}
