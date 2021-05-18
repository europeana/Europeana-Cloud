package eu.europeana.cloud.service.dps.storm.utils;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.dps.AttributeStatistics;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;

import java.util.HashSet;
import java.util.Set;

public class CassandraAttributeStatisticsDAO extends CassandraDAO {
    public static final int MAX_ALLOWED_VALUES = 15;
    private static CassandraAttributeStatisticsDAO instance = null;

    private PreparedStatement updateAttributeStatement;

    private PreparedStatement selectAttributesStatement;
    private PreparedStatement deleteAttributesStatement;

    private PreparedStatement countDistinctAttributeValues;

    private PreparedStatement countSpecificAttributeValue;

    public static synchronized CassandraAttributeStatisticsDAO getInstance(CassandraConnectionProvider cassandra) {
        if (instance == null) {
            instance = RetryableMethodExecutor.createRetryProxy(new CassandraAttributeStatisticsDAO(cassandra));
        }
        return instance;
    }

    /**
     * @param dbService The service exposing the connection and session
     */
    public CassandraAttributeStatisticsDAO(CassandraConnectionProvider dbService) {
        super(dbService);
    }

    public CassandraAttributeStatisticsDAO() {
    }

    @Override
    void prepareStatements() {
        updateAttributeStatement = dbService.getSession().prepare("UPDATE " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_TABLE +
                " SET " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_OCCURRENCE + " = " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_OCCURRENCE + " + ? " +
                "WHERE " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_TASK_ID + " = ? " +
                "AND " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_NODE_XPATH + " = ? " +
                "AND " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_NODE_VALUE + " = ? " +
                "AND " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_NAME + " = ? " +
                "AND " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_VALUE + " = ?");
        updateAttributeStatement.setConsistencyLevel(dbService.getConsistencyLevel());

        selectAttributesStatement = dbService.getSession().prepare("SELECT * FROM " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_TABLE +
                " WHERE " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_TASK_ID + " = ? " +
                "AND " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_NODE_XPATH + " = ? " +
                "AND " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_NODE_VALUE + " = ? LIMIT ?");
        selectAttributesStatement.setConsistencyLevel(dbService.getConsistencyLevel());

        deleteAttributesStatement = dbService.getSession().prepare("DELETE FROM " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_TABLE +
                " WHERE " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_TASK_ID + " = ? " +
                "AND " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_NODE_XPATH + " = ? " +
                "AND " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_NODE_VALUE + " = ?");
        deleteAttributesStatement.setConsistencyLevel(dbService.getConsistencyLevel());

        countDistinctAttributeValues = dbService.getSession().prepare("SELECT count(*)" +
                " FROM " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_TABLE +
                " WHERE " + CassandraTablesAndColumnsNames.NODE_STATISTICS_TASK_ID + " = ? " +
                "AND " + CassandraTablesAndColumnsNames.NODE_STATISTICS_NODE_XPATH + " = ? " +
                "AND " + CassandraTablesAndColumnsNames.NODE_STATISTICS_VALUE + " = ? " +
                "AND " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_NAME + " = ?");
        countDistinctAttributeValues.setConsistencyLevel(dbService.getConsistencyLevel());

        countSpecificAttributeValue = dbService.getSession().prepare("SELECT count(*)" +
                " FROM " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_TABLE +
                " WHERE " + CassandraTablesAndColumnsNames.NODE_STATISTICS_TASK_ID + " = ? " +
                "AND " + CassandraTablesAndColumnsNames.NODE_STATISTICS_NODE_XPATH + " = ? " +
                "AND " + CassandraTablesAndColumnsNames.NODE_STATISTICS_VALUE + " = ? " +
                "AND " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_NAME + " = ? " +
                "AND " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_VALUE + " = ?");
        countSpecificAttributeValue.setConsistencyLevel(dbService.getConsistencyLevel());
    }

    /**
     * Inserts the statistics for all the attributes in the given list.
     *
     * @param taskId     task identifier
     * @param attributes list of attribute statistics
     */
    public void insertAttributeStatistics(long taskId, String nodeXpath, String nodeValue, Set<AttributeStatistics> attributes) {
        for (AttributeStatistics attributeStatistics : attributes) {
            long distinctValuesCount = getAttributeDistinctValues(taskId, nodeXpath, nodeValue, attributeStatistics.getName());
            if (distinctValuesCount >= MAX_ALLOWED_VALUES) {
                long currentCount = getSpecificAttributeValueCount(taskId, nodeXpath, nodeValue, attributeStatistics.getName(), attributeStatistics.getValue());
                if (currentCount > 0)
                    insertAttributeStatistics(taskId, nodeXpath, nodeValue, attributeStatistics);
            } else {
                insertAttributeStatistics(taskId, nodeXpath, nodeValue, attributeStatistics);
            }
        }
    }

    private long getAttributeDistinctValues(long taskId, String nodePath, String nodeValue, String attributeName) {
        ResultSet rs = dbService.getSession().execute(countDistinctAttributeValues.bind(taskId, nodePath, nodeValue, attributeName));
        return rs.one().getLong(0);
    }

    private long getSpecificAttributeValueCount(long taskId, String nodePath, String nodeValue, String attributeName, String attributeValue) {
        ResultSet rs = dbService.getSession().execute(countSpecificAttributeValue.bind(taskId, nodePath, nodeValue, attributeName, attributeValue));
        return rs.one().getLong(0);
    }

    /**
     * Inserts the statistics for the specified attribute.
     *
     * @param taskId              task identifier
     * @param attributeStatistics attribute statistics to insert
     */
    public void insertAttributeStatistics(long taskId, String nodeXpath, String nodeValue, AttributeStatistics attributeStatistics) {
        dbService.getSession().execute(updateAttributeStatement.bind(attributeStatistics.getOccurrence(),
                taskId,
                nodeXpath,
                nodeValue,
                attributeStatistics.getName(),
                attributeStatistics.getValue()));
    }

    /**
     * Retrieve a list of attribute statistics for the specified task and node
     *
     * @param taskId    task identifier
     * @param nodeXpath node xpath that contains returned attributes
     * @return list of attribute statistics objects
     */
    public Set<AttributeStatistics> getAttributeStatistics(long taskId, String nodeXpath, String nodeValue, int limit) {
        BoundStatement bs = selectAttributesStatement.bind(taskId, nodeXpath, nodeValue, limit);
        ResultSet rs = dbService.getSession().execute(bs);
        Set<AttributeStatistics> result = new HashSet<>();

        while (rs.iterator().hasNext()) {
            Row row = rs.one();
            result.add(new AttributeStatistics(row.getString(CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_NAME),
                    row.getString(CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_VALUE),
                    row.getLong(CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_OCCURRENCE)));
        }

        return result;
    }

    public void removeAttributeStatistics(long taskId, String nodeXpath, String nodeValue) {
        BoundStatement bs = deleteAttributesStatement.bind(taskId, nodeXpath, nodeValue);
        dbService.getSession().execute(bs);
    }


}
