package eu.europeana.cloud.service.dps.storm.utils;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.gson.Gson;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.dps.AttributeStatistics;
import eu.europeana.cloud.common.model.dps.NodeStatistics;
import eu.europeana.cloud.common.model.dps.NodeReport;
import eu.europeana.cloud.common.model.dps.StatisticsReport;

import java.util.ArrayList;
import java.util.List;

public class CassandraNodeStatisticsDAO extends CassandraDAO {
    public static final int ELEMETNS_SAMPLE_MAX_SIZE = 100;
    public static final int ATTRIBUTES_SAMPLE_MAX_SIZE = 25;
    private final Gson gson = new Gson();

    private PreparedStatement updateStatement;

    private PreparedStatement updateNodeStatement;

    private PreparedStatement searchByParentStatement;

    private PreparedStatement searchGeneralStatistcsByTaskIdStatement;

    private PreparedStatement removeGeneralStatisticsStatement;

    private PreparedStatement searchByNodeStatement;
    private PreparedStatement searchByAttributeStatement;

    private PreparedStatement searchNodesStatement;

    private PreparedStatement searchNodesStatementAll;

    private PreparedStatement deleteNodesStatistcsStatement;

    private PreparedStatement getStatisticsReportStatement;
    private PreparedStatement removeStatisticsReportStatement;

    private PreparedStatement checkStatisticsReportStatement;

    private PreparedStatement storeStatisticsReportStatement;

    private CassandraAttributeStatisticsDAO cassandraAttributeStatisticsDAO;

    private static CassandraNodeStatisticsDAO instance = null;

    public static synchronized CassandraNodeStatisticsDAO getInstance(CassandraConnectionProvider cassandra) {
        if (instance == null) {
            instance = new CassandraNodeStatisticsDAO(cassandra);
        }
        return instance;
    }

    /**
     * @param dbService The service exposing the connection and session
     */
    public CassandraNodeStatisticsDAO(CassandraConnectionProvider dbService) {
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

        searchGeneralStatistcsByTaskIdStatement = dbService.getSession().prepare("SELECT *" +
                " FROM " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_TABLE +
                " WHERE " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_TASK_ID + " = ?");
        searchGeneralStatistcsByTaskIdStatement.setConsistencyLevel(dbService.getConsistencyLevel());


        removeGeneralStatisticsStatement = dbService.getSession().prepare("DELETE " +
                " FROM " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_TABLE +
                " WHERE " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_TASK_ID + " = ?");
        removeGeneralStatisticsStatement.setConsistencyLevel(dbService.getConsistencyLevel());

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


        searchByAttributeStatement = dbService.getSession().prepare("SELECT *" +
                " FROM " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_TABLE +
                " WHERE " + CassandraTablesAndColumnsNames.NODE_STATISTICS_TASK_ID + " = ? " +
                "AND " + CassandraTablesAndColumnsNames.NODE_STATISTICS_NODE_XPATH + " = ? " +
                "AND " + CassandraTablesAndColumnsNames.NODE_STATISTICS_VALUE + " = ? limit ?");
        searchByAttributeStatement.setConsistencyLevel(dbService.getConsistencyLevel());


        checkStatisticsReportStatement = dbService.getSession().prepare("SELECT " + CassandraTablesAndColumnsNames.STATISTICS_REPORTS_TASK_ID +
                " FROM " + CassandraTablesAndColumnsNames.STATISTICS_REPORTS_TABLE +
                " WHERE " + CassandraTablesAndColumnsNames.STATISTICS_REPORTS_TASK_ID + " = ?");
        checkStatisticsReportStatement.setConsistencyLevel(dbService.getConsistencyLevel());

        getStatisticsReportStatement = dbService.getSession().prepare("SELECT blobastext(" + CassandraTablesAndColumnsNames.STATISTICS_REPORTS_REPORT_DATA + ")" +
                " FROM " + CassandraTablesAndColumnsNames.STATISTICS_REPORTS_TABLE +
                " WHERE " + CassandraTablesAndColumnsNames.STATISTICS_REPORTS_TASK_ID + " = ?");
        getStatisticsReportStatement.setConsistencyLevel(dbService.getConsistencyLevel());


        removeStatisticsReportStatement = dbService.getSession().prepare("DELETE " +
                " FROM " + CassandraTablesAndColumnsNames.STATISTICS_REPORTS_TABLE +
                " WHERE " + CassandraTablesAndColumnsNames.STATISTICS_REPORTS_TASK_ID + " = ?");
        removeStatisticsReportStatement.setConsistencyLevel(dbService.getConsistencyLevel());

        storeStatisticsReportStatement = dbService.getSession().prepare("INSERT INTO " + CassandraTablesAndColumnsNames.STATISTICS_REPORTS_TABLE +
                " (" + CassandraTablesAndColumnsNames.STATISTICS_REPORTS_TASK_ID + "," +
                CassandraTablesAndColumnsNames.STATISTICS_REPORTS_REPORT_DATA + ")" +
                " VALUES (?,textasblob(?))");
        storeStatisticsReportStatement.setConsistencyLevel(dbService.getConsistencyLevel());
    }

    /**
     * Insert statistics for all nodes on the list
     *
     * @param taskId task identifier
     * @param nodes  list of node statistics objects
     */
    public void insertNodeStatistics(long taskId, List<NodeStatistics> nodes) {
        for (NodeStatistics nodeStatistics : nodes) {
            insertNodeStatistics(taskId, nodeStatistics);
        }
    }

    /**
     * Insert statistics for the specified node
     *
     * @param taskId         task identifier
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
     *
     * @param taskId         task identifier
     * @param nodeStatistics node statistics object to store / update
     */
    private void updateNodeStatistics(long taskId, NodeStatistics nodeStatistics) {
        dbService.getSession().execute(updateNodeStatement.bind(nodeStatistics.getOccurrence(), taskId, nodeStatistics.getXpath(), nodeStatistics.getValue()));
    }

    /**
     * It will update the counter for the specified node in general statistics table
     *
     * @param taskId         task identifier
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
        ResultSet rs = dbService.getSession().execute(searchGeneralStatistcsByTaskIdStatement.bind(taskId));
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
        ResultSet rs = dbService.getSession().execute(searchNodesStatement.bind(taskId, nodeXpath, 2));

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
     * @param taskId      task identifier
     * @param parentXpath xpath of the parent
     * @param nodeXpath   xpath of the node
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

    /**
     * Check whether report for the specific task has already been stored.
     *
     * @param taskId task identifier
     * @return true when a row for the given task identifier is in the table
     */
    public boolean isReportStored(long taskId) {
        BoundStatement bs = checkStatisticsReportStatement.bind(taskId);
        ResultSet rs = dbService.getSession().execute(bs);

        return rs.iterator().hasNext();
    }

    /**
     * Store the StatisticsReport object in the database.
     *
     * @param taskId task identifier
     * @param report report object to store
     */
    public void storeStatisticsReport(long taskId, StatisticsReport report) {
        if (isReportStored(taskId)) {
            return;
        }
        String reportSerialized = gson.toJson(report);
        if (reportSerialized != null) {
            BoundStatement bs = storeStatisticsReportStatement.bind(taskId, reportSerialized);
            dbService.getSession().execute(bs);
        }
    }

    /**
     * Return statistics report from the database. When not present null will be returned.
     *
     * @param taskId task identifier
     * @return statistics report object
     */
    public StatisticsReport getStatisticsReport(long taskId) {
        BoundStatement bs = getStatisticsReportStatement.bind(taskId);
        ResultSet rs = dbService.getSession().execute(bs);

        if (rs.iterator().hasNext()) {
            Row row = rs.one();

            String report = row.getString(0);
            return gson.fromJson(report, StatisticsReport.class);
        }
        return null;
    }


    public List<NodeReport> getElementReport(long taskId, String nodeXpath) {
        List<NodeReport> result = new ArrayList<>();
        ResultSet rs = dbService.getSession().execute(searchNodesStatement.bind(taskId, nodeXpath, ELEMETNS_SAMPLE_MAX_SIZE));
        while (rs.iterator().hasNext()) {
            Row elementRow = rs.one();
            String elementValue = elementRow.getString(CassandraTablesAndColumnsNames.NODE_STATISTICS_VALUE);
            List<AttributeStatistics> attributeStatistics = getAttributesStatistics(taskId, nodeXpath, elementValue);
            NodeReport nodeValues = new NodeReport(elementValue, elementRow.getLong(CassandraTablesAndColumnsNames.NODE_STATISTICS_OCCURRENCE), attributeStatistics);
            result.add(nodeValues);
        }
        return result;
    }


    public void removeStatistics(long taskId) {
        removeGeneralStatistics(taskId);
        dbService.getSession().execute(removeStatisticsReportStatement.bind(taskId));
    }


    public void removeGeneralStatistics(long taskId) {
        ResultSet rs = dbService.getSession().execute(searchGeneralStatistcsByTaskIdStatement.bind(taskId));
        while (rs.iterator().hasNext()) {
            Row row = rs.one();
            String nodeXpath = row.getString(CassandraTablesAndColumnsNames.GENERAL_STATISTICS_NODE_XPATH);
            removeNodeStatistics(taskId, nodeXpath);
        }
        dbService.getSession().execute(removeGeneralStatisticsStatement.bind(taskId));
    }

    private void removeNodeStatistics(long taskId, String nodeXpath) {
        ResultSet rs = dbService.getSession().execute(searchNodesStatementAll.bind(taskId, nodeXpath));
        while (rs.iterator().hasNext()) {
            Row row = rs.one();
            cassandraAttributeStatisticsDAO.removeAttributeStatistics(taskId, nodeXpath, row.getString(CassandraTablesAndColumnsNames.NODE_STATISTICS_VALUE));
        }
        dbService.getSession().execute(deleteNodesStatistcsStatement.bind(taskId, nodeXpath));
    }


    private List<AttributeStatistics> getAttributesStatistics(long taskId, String nodeXpath, String elementValue) {
        List<AttributeStatistics> attributeStatistics = new ArrayList<>();
        ResultSet attributeRs = dbService.getSession().execute(searchByAttributeStatement.bind(taskId, nodeXpath, elementValue, ATTRIBUTES_SAMPLE_MAX_SIZE));
        while (attributeRs.iterator().hasNext()) {
            Row attributeRow = attributeRs.one();
            attributeStatistics.add(new AttributeStatistics(attributeRow.getString(CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_NAME), attributeRow.getString(CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_VALUE),
                    attributeRow.getLong(CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_OCCURRENCE)));
        }
        return attributeStatistics;
    }
}
