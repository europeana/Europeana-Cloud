package eu.europeana.cloud.service.dps.storm.utils;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.dps.NodeStatistics;

import java.util.ArrayList;
import java.util.List;

public class CassandraNodeStatisticsDAO extends CassandraDAO {
    private PreparedStatement updateStatement;

    private PreparedStatement updateNodeStatement;

    private PreparedStatement searchByParentStatement;

    private PreparedStatement searchByTaskIdStatement;

    private PreparedStatement searchByNodeStatement;

    private PreparedStatement searchNodesStatement;

    private CassandraAttributeStatisticsDAO cassandraAttributeStatisticsDAO;

    private static CassandraNodeStatisticsDAO instance = null;

    public static CassandraNodeStatisticsDAO getInstance(CassandraConnectionProvider cassandra) {
        if (instance == null) {
            synchronized (CassandraNodeStatisticsDAO.class) {
                if (instance == null) {
                    instance = new CassandraNodeStatisticsDAO(cassandra);
                }
            }
        }
        return instance;
    }

    /**
     * @param dbService The service exposing the connection and session
     */
    private CassandraNodeStatisticsDAO(CassandraConnectionProvider dbService) {
        super(dbService);
        cassandraAttributeStatisticsDAO = CassandraAttributeStatisticsDAO.getInstance(dbService);
    }

    @Override
    void prepareStatements() {
        updateStatement = dbService.getSession().prepare("UPDATE " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_TABLE +
                " SET " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_OCCURRENCE + " = " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_OCCURRENCE + " + 1 " +
                "WHERE " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_TASK_ID + " = ? " +
                "AND " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_PARENT_XPATH + " = ? " +
                "AND " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_NODE_XPATH + " = ?");
        updateStatement.setConsistencyLevel(dbService.getConsistencyLevel());

        updateNodeStatement = dbService.getSession().prepare("UPDATE " + CassandraTablesAndColumnsNames.NODE_STATISTICS_TABLE +
                " SET " + CassandraTablesAndColumnsNames.NODE_STATISTICS_OCCURRENCE + " = " + CassandraTablesAndColumnsNames.NODE_STATISTICS_OCCURRENCE + " + ? " +
                "WHERE " + CassandraTablesAndColumnsNames.NODE_STATISTICS_TASK_ID + " = ? " +
                "AND " + CassandraTablesAndColumnsNames.NODE_STATISTICS_NODE_XPATH + " = ? " +
                "AND " + CassandraTablesAndColumnsNames.NODE_STATISTICS_VALUE + " = ?");
        updateNodeStatement.setConsistencyLevel(dbService.getConsistencyLevel());

        searchByTaskIdStatement = dbService.getSession().prepare("SELECT *" +
                " FROM " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_TABLE +
                " WHERE " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_TASK_ID + " = ?");
        searchByTaskIdStatement.setConsistencyLevel(dbService.getConsistencyLevel());

        searchByParentStatement = dbService.getSession().prepare("SELECT *" +
                " FROM " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_TABLE +
                " WHERE " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_TASK_ID + " = ? " +
                "AND " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_PARENT_XPATH + " = ?");
        searchByParentStatement.setConsistencyLevel(dbService.getConsistencyLevel());

        searchByNodeStatement = dbService.getSession().prepare("SELECT *" +
                " FROM " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_TABLE +
                " WHERE " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_TASK_ID + " = ? " +
                "AND " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_PARENT_XPATH + " = ? " +
                "AND " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_NODE_XPATH + " = ?");
        searchByNodeStatement.setConsistencyLevel(dbService.getConsistencyLevel());

        searchNodesStatement = dbService.getSession().prepare("SELECT *" +
                " FROM " + CassandraTablesAndColumnsNames.NODE_STATISTICS_TABLE +
                " WHERE " + CassandraTablesAndColumnsNames.NODE_STATISTICS_TASK_ID + " = ? " +
                "AND " + CassandraTablesAndColumnsNames.NODE_STATISTICS_NODE_XPATH + " = ?");
        searchNodesStatement.setConsistencyLevel(dbService.getConsistencyLevel());
    }

    /**
     * Insert statistics for all nodes on the list
     *
     * @param taskId task identifier
     * @param nodes list of node statistics objects
     */
    public void insertNodeStatistics(long taskId, List<NodeStatistics> nodes) {
        for (NodeStatistics nodeStatistics : nodes) {
            insertNodeStatistics(taskId, nodeStatistics);
        }
    }

    /**
     * Insert statistics for the specified node
     * @param taskId task identifier
     * @param nodeStatistics node statistics to insert
     */
    public void insertNodeStatistics(long taskId, NodeStatistics nodeStatistics) {
        updateGeneralStatistics(taskId, nodeStatistics);
        // store node statistics only for nodes with values, occurrence of the node itself will be taken from the general statistics
        if (nodeStatistics.getValue() != null) {
            updateNodeStatistics(taskId, nodeStatistics);
        }
        if (nodeStatistics.hasAttributes()) {
            cassandraAttributeStatisticsDAO.insertAttributeStatistics(taskId, nodeStatistics.getXpath(), nodeStatistics.getValue(), nodeStatistics.getAttributesStatistics());
        }
    }

    /**
     * Update counter in the node statistics table
     * @param taskId task identifier
     * @param nodeStatistics node statistics object to store / update
     */
    private void updateNodeStatistics(long taskId, NodeStatistics nodeStatistics) {
        dbService.getSession().execute(updateNodeStatement.bind(nodeStatistics.getOccurrence(), taskId, nodeStatistics.getXpath(), nodeStatistics.getValue()));
    }

    /**
     * It will update the counter for the specified node in general statistics table
     * @param taskId task identifier
     * @param nodeStatistics node statistics object with all the necessary information
     */
    private void updateGeneralStatistics(long taskId, NodeStatistics nodeStatistics) {
        dbService.getSession().execute(updateStatement.bind(taskId, nodeStatistics.getParentXpath(), nodeStatistics.getXpath()));
    }

    /**
     * Get all statistics for the specified task
     *
     * @param taskId task identifier
     * @return list of all node statistics
     */
    public List<NodeStatistics> getNodeStatistics(long taskId) {
        ResultSet rs = dbService.getSession().execute(searchByTaskIdStatement.bind(taskId));
        List<NodeStatistics> result = new ArrayList<>();

        while (rs.iterator().hasNext()) {
            Row row = rs.one();
            String parentXpath = row.getString(CassandraTablesAndColumnsNames.GENERAL_STATISTICS_PARENT_XPATH);
            String nodeXpath = row.getString(CassandraTablesAndColumnsNames.GENERAL_STATISTICS_NODE_XPATH);

            List<NodeStatistics> nodeStatistics = retrieveNodeStatistics(taskId, parentXpath, nodeXpath);
            if (nodeStatistics.isEmpty()) {
                // this case happens when there is a node without a value but it may contain attributes
                Long occurrence = row.getLong(CassandraTablesAndColumnsNames.GENERAL_STATISTICS_OCCURRENCE);
                NodeStatistics node = new NodeStatistics(parentXpath, nodeXpath, "", occurrence);
                node.setAttributesStatistics(cassandraAttributeStatisticsDAO.getAttributeStatistics(taskId, nodeXpath, ""));
            }
            result.addAll(nodeStatistics);
        }
        return result;
    }

    private List<NodeStatistics> retrieveNodeStatistics(long taskId, String parentXpath, String nodeXpath) {
        List<NodeStatistics> result = new ArrayList<>();
        ResultSet rs = dbService.getSession().execute(searchNodesStatement.bind(taskId, nodeXpath));

        while (rs.iterator().hasNext()) {
            Row row = rs.one();

            NodeStatistics nodeStatistics = new NodeStatistics(parentXpath, nodeXpath, row.getString(CassandraTablesAndColumnsNames.NODE_STATISTICS_VALUE), row.getLong(CassandraTablesAndColumnsNames.NODE_STATISTICS_OCCURRENCE));
            nodeStatistics.setAttributesStatistics(cassandraAttributeStatisticsDAO.getAttributeStatistics(taskId, nodeXpath, nodeStatistics.getValue()));
            result.add(nodeStatistics);
        }
        return result;
    }

    /**
     * Retrieve the list of node statistics according to the given filters. When nodeXpath is null then all
     * node statistics that have the given parentXpath will be returned. If parentXpath is null then all root
     * node statistics will be returned. If you want to get all nodes for the whole task you should use another
     * method which takes as input just the task identifier.
     *
     * @param taskId task identifier
     * @param parentXpath xpath of the parent
     * @param nodeXpath xpath of the node
     * @return list of node statistics
     */
    public List<NodeStatistics> getNodeStatistics(long taskId, String parentXpath, String nodeXpath) {
        BoundStatement bs;
        if (nodeXpath.isEmpty()) {
            bs = searchByParentStatement.bind(taskId, parentXpath);
        } else {
            bs = searchByNodeStatement.bind(taskId, parentXpath, nodeXpath);
        }

        ResultSet rs = dbService.getSession().execute(bs);
        List<NodeStatistics> result = new ArrayList<>();

        while (rs.iterator().hasNext()) {
            Row row = rs.one();

            result.addAll(retrieveNodeStatistics(taskId,
                    row.getString(CassandraTablesAndColumnsNames.GENERAL_STATISTICS_PARENT_XPATH),
                    row.getString(CassandraTablesAndColumnsNames.GENERAL_STATISTICS_NODE_XPATH)));
        }
        return result;
    }
}
