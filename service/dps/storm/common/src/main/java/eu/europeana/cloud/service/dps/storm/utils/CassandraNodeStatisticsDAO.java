package eu.europeana.cloud.service.dps.storm.utils;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.annotation.Retryable;
import eu.europeana.cloud.common.model.dps.NodeStatistics;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;

import java.util.List;
import java.util.stream.Collectors;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyDefaultsConstants.DPS_DEFAULT_MAX_ATTEMPTS;

public class CassandraNodeStatisticsDAO extends CassandraDAO {

    private PreparedStatement updateNodeStatement;

    private PreparedStatement searchNodesStatement;

    private PreparedStatement searchNodesStatementAll;

    private PreparedStatement deleteNodesStatisticsStatement;

    private static CassandraNodeStatisticsDAO instance = null;

    public static synchronized CassandraNodeStatisticsDAO getInstance(CassandraConnectionProvider cassandra) {
        if (instance == null) {
            instance = RetryableMethodExecutor.createRetryProxy(new CassandraNodeStatisticsDAO(cassandra));
        }
        return instance;
    }

    /**
     * @param dbService The service exposing the connection and session
     */
    public CassandraNodeStatisticsDAO(CassandraConnectionProvider dbService) {
        super(dbService);
    }

    public CassandraNodeStatisticsDAO() {
    }

    @Override
    void prepareStatements() {
        updateNodeStatement = dbService.getSession().prepare("UPDATE " + CassandraTablesAndColumnsNames.NODE_STATISTICS_TABLE +
                " SET " + CassandraTablesAndColumnsNames.NODE_STATISTICS_OCCURRENCE + " = " + CassandraTablesAndColumnsNames.NODE_STATISTICS_OCCURRENCE + " + ? " +
                "WHERE " + CassandraTablesAndColumnsNames.NODE_STATISTICS_TASK_ID + " = ? " +
                "AND " + CassandraTablesAndColumnsNames.NODE_STATISTICS_NODE_XPATH + " = ? " +
                "AND " + CassandraTablesAndColumnsNames.NODE_STATISTICS_VALUE + " = ?");
        updateNodeStatement.setConsistencyLevel(dbService.getConsistencyLevel());


        searchNodesStatement = dbService.getSession().prepare("SELECT *" +
                " FROM " + CassandraTablesAndColumnsNames.NODE_STATISTICS_TABLE +
                " WHERE " + CassandraTablesAndColumnsNames.NODE_STATISTICS_TASK_ID + " = ? " +
                "AND " + CassandraTablesAndColumnsNames.NODE_STATISTICS_NODE_XPATH + " = ? limit ?");
        searchNodesStatement.setConsistencyLevel(dbService.getConsistencyLevel());


        searchNodesStatementAll = dbService.getSession().prepare("SELECT *" +
                " FROM " + CassandraTablesAndColumnsNames.NODE_STATISTICS_TABLE +
                " WHERE " + CassandraTablesAndColumnsNames.NODE_STATISTICS_TASK_ID + " = ? " +
                "AND " + CassandraTablesAndColumnsNames.NODE_STATISTICS_NODE_XPATH + " = ?");
        searchNodesStatementAll.setConsistencyLevel(dbService.getConsistencyLevel());

        deleteNodesStatisticsStatement = dbService.getSession().prepare("DELETE " +
                " FROM " + CassandraTablesAndColumnsNames.NODE_STATISTICS_TABLE +
                " WHERE " + CassandraTablesAndColumnsNames.NODE_STATISTICS_TASK_ID + " = ? " +
                "AND " + CassandraTablesAndColumnsNames.NODE_STATISTICS_NODE_XPATH + " = ?");
        deleteNodesStatisticsStatement.setConsistencyLevel(dbService.getConsistencyLevel());


    }

    /**
     * Update counter in the node statistics table
     *
     * @param taskId         task identifier
     * @param nodeStatistics node statistics object to store / update
     */
    @Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS)
    public void updateNodeStatistics(long taskId, NodeStatistics nodeStatistics) {
        dbService.getSession().execute(updateNodeStatement.bind(
                nodeStatistics.getOccurrence(), taskId, nodeStatistics.getXpath(), nodeStatistics.getValue()));
    }

    @Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS)
    public List<String> searchNodeStatisticsValues(long taskId, String nodeXpath) {
        return dbService.getSession().execute(searchNodesStatementAll.bind(taskId, nodeXpath)).all().stream()
                .map(row -> row.getString(CassandraTablesAndColumnsNames.NODE_STATISTICS_VALUE))
                .collect(Collectors.toList());
    }

    @Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS)
    public void removeNodeStatistics(long taskId, String nodeXpath) {
        dbService.getSession().execute(deleteNodesStatisticsStatement.bind(taskId, nodeXpath));
    }

    @Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS)
    public List<NodeStatistics> getNodeStatistics(long taskId, String parentXpath, String nodeXpath, int limit) {
        return dbService.getSession().execute(searchNodesStatement.bind(taskId, nodeXpath, limit)).all().stream()
                .map(row -> createNodeStatistics(parentXpath, nodeXpath, row))
                .collect(Collectors.toList());
    }

    private NodeStatistics createNodeStatistics(String parentXpath, String nodeXpath, Row row) {
        return new NodeStatistics(parentXpath, nodeXpath,
                row.getString(CassandraTablesAndColumnsNames.NODE_STATISTICS_VALUE),
                row.getLong(CassandraTablesAndColumnsNames.NODE_STATISTICS_OCCURRENCE));
    }

}
