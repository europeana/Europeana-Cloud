package eu.europeana.cloud.service.dps.storm.utils;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.dps.AttributeStatistics;

import java.util.HashSet;
import java.util.Set;

public class CassandraAttributeStatisticsDAO extends CassandraDAO {
    private static CassandraAttributeStatisticsDAO instance = null;

    private PreparedStatement updateAttributeStatement;

    private PreparedStatement selectAttributesStatement;

    public static synchronized CassandraAttributeStatisticsDAO getInstance(CassandraConnectionProvider cassandra) {
        if (instance == null) {
            instance = new CassandraAttributeStatisticsDAO(cassandra);
        }
        return instance;
    }

    /**
     * @param dbService The service exposing the connection and session
     */
    private CassandraAttributeStatisticsDAO(CassandraConnectionProvider dbService) {
        super(dbService);
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
                "AND " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_NODE_VALUE + " = ? LIMIT 2");
        selectAttributesStatement.setConsistencyLevel(dbService.getConsistencyLevel());
    }

    /**
     * Inserts the statistics for all the attributes in the given list.
     *
     * @param taskId     task identifier
     * @param attributes list of attribute statistics
     */
    public void insertAttributeStatistics(long taskId, String nodeXpath, String nodeValue, Set<AttributeStatistics> attributes) {
        for (AttributeStatistics attributeStatistics : attributes) {
            insertAttributeStatistics(taskId, nodeXpath, nodeValue, attributeStatistics);
        }
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
    public Set<AttributeStatistics> getAttributeStatistics(long taskId, String nodeXpath, String nodeValue) {
        BoundStatement bs = selectAttributesStatement.bind(taskId, nodeXpath, nodeValue);
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
}
