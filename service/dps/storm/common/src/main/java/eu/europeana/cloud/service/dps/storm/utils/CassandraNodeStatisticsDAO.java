package eu.europeana.cloud.service.dps.storm.utils;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.dps.NodeStatistics;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;

import java.util.ArrayList;
import java.util.List;

public class CassandraNodeStatisticsDAO extends CassandraDAO {

    private PreparedStatement updateNodeStatement;

    private PreparedStatement searchNodesStatement;

    private PreparedStatement searchNodesStatementAll;

    private PreparedStatement deleteNodesStatistcsStatement;

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

        deleteNodesStatistcsStatement = dbService.getSession().prepare("DELETE " +
                " FROM " + CassandraTablesAndColumnsNames.NODE_STATISTICS_TABLE +
                " WHERE " + CassandraTablesAndColumnsNames.NODE_STATISTICS_TASK_ID + " = ? " +
                "AND " + CassandraTablesAndColumnsNames.NODE_STATISTICS_NODE_XPATH + " = ?");
        deleteNodesStatistcsStatement.setConsistencyLevel(dbService.getConsistencyLevel());



    }
    /**
     * Update counter in the node statistics table
     *
     * @param taskId         task identifier
     * @param nodeStatistics node statistics object to store / update
     */
    public void updateNodeStatistics(long taskId, NodeStatistics nodeStatistics) {
        dbService.getSession().execute(updateNodeStatement.bind(nodeStatistics.getOccurrence(), taskId, nodeStatistics.getXpath(), nodeStatistics.getValue()));
    }

    public List<String> searchNodeStatisticsValues(long taskId, String nodeXpath) {
        List<String> result = new ArrayList<>();
        ResultSet rs = dbService.getSession().execute(searchNodesStatementAll.bind(taskId, nodeXpath));

        while (rs.iterator().hasNext()) {
            Row row = rs.one();
            result.add(row.getString(CassandraTablesAndColumnsNames.NODE_STATISTICS_VALUE));
        }
        return result;
    }

    public void removeNodeStatistics(long taskId, String nodeXpath) {
        dbService.getSession().execute(deleteNodesStatistcsStatement.bind(taskId, nodeXpath));
    }

    public List<NodeStatistics> retrieveNodeStatistics(long taskId, String parentXpath, String nodeXpath, int limit) {
        List<NodeStatistics> result = new ArrayList<>();
        ResultSet rs = dbService.getSession().execute(searchNodesStatement.bind(taskId, nodeXpath, limit));

        while (rs.iterator().hasNext()) {
            Row row = rs.one();

            NodeStatistics nodeStatistics = new NodeStatistics(parentXpath, nodeXpath,
                    row.getString(CassandraTablesAndColumnsNames.NODE_STATISTICS_VALUE),
                    row.getLong(CassandraTablesAndColumnsNames.NODE_STATISTICS_OCCURRENCE));
            result.add(nodeStatistics);
        }
        return result;
    }

}
