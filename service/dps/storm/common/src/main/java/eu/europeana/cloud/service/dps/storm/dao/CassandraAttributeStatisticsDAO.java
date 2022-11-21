package eu.europeana.cloud.service.dps.storm.dao;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.annotation.Retryable;
import eu.europeana.cloud.common.model.dps.AttributeStatistics;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames;

import java.util.Set;
import java.util.stream.Collectors;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyDefaultsConstants.DPS_DEFAULT_MAX_ATTEMPTS;

@Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS)
public class CassandraAttributeStatisticsDAO extends CassandraDAO {

  private static CassandraAttributeStatisticsDAO instance = null;

  private PreparedStatement updateAttributeStatement;

  private PreparedStatement selectAttributesStatement;
  private PreparedStatement deleteAttributesStatement;

  private PreparedStatement countDistinctAttributeValues;

  private PreparedStatement countSpecificAttributeValue;

  /**
   * @param dbService The service exposing the connection and session
   */
  public CassandraAttributeStatisticsDAO(CassandraConnectionProvider dbService) {
    super(dbService);
  }

  public CassandraAttributeStatisticsDAO() {
    //needed for creating cglib proxy in RetryableMethodExecutor.createRetryProxy()
  }

  public static synchronized CassandraAttributeStatisticsDAO getInstance(CassandraConnectionProvider cassandra) {
    if (instance == null) {
      instance = RetryableMethodExecutor.createRetryProxy(new CassandraAttributeStatisticsDAO(cassandra));
    }
    return instance;
  }


  @Override
  protected void prepareStatements() {
    updateAttributeStatement = dbService.getSession().prepare(
        "UPDATE " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_TABLE
            + " SET " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_OCCURRENCE + " = "
            + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_OCCURRENCE + " + ? "
            + "WHERE " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_TASK_ID + " = ? "
            + "AND " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_NODE_XPATH + " = ? "
            + "AND " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_NODE_VALUE + " = ? "
            + "AND " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_NAME + " = ? "
            + "AND " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_VALUE + " = ?"
    );

    selectAttributesStatement = dbService.getSession().prepare(
        "SELECT * "
            + "FROM " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_TABLE
            + " WHERE " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_TASK_ID + " = ? "
            + "AND " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_NODE_XPATH + " = ? "
            + "AND " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_NODE_VALUE + " = ? LIMIT ?"
    );

    deleteAttributesStatement = dbService.getSession().prepare(
        "DELETE FROM " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_TABLE
            + " WHERE " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_TASK_ID + " = ? "
            + "AND " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_NODE_XPATH + " = ? "
            + "AND " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_NODE_VALUE + " = ?"
    );

    countDistinctAttributeValues = dbService.getSession().prepare(
        "SELECT count(*)"
            + " FROM " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_TABLE
            + " WHERE " + CassandraTablesAndColumnsNames.NODE_STATISTICS_TASK_ID + " = ? "
            + "AND " + CassandraTablesAndColumnsNames.NODE_STATISTICS_NODE_XPATH + " = ? "
            + "AND " + CassandraTablesAndColumnsNames.NODE_STATISTICS_VALUE + " = ? "
            + "AND " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_NAME + " = ?"
    );

    countSpecificAttributeValue = dbService.getSession().prepare(
        "SELECT count(*)"
            + " FROM " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_TABLE
            + " WHERE " + CassandraTablesAndColumnsNames.NODE_STATISTICS_TASK_ID + " = ? "
            + "AND " + CassandraTablesAndColumnsNames.NODE_STATISTICS_NODE_XPATH + " = ? "
            + "AND " + CassandraTablesAndColumnsNames.NODE_STATISTICS_VALUE + " = ? "
            + "AND " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_NAME + " = ? "
            + "AND " + CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_VALUE + " = ?"
    );
  }


  public long getAttributeDistinctValues(long taskId, String nodePath, String nodeValue, String attributeName) {
    ResultSet rs = dbService.getSession().execute(countDistinctAttributeValues.bind(taskId, nodePath, nodeValue, attributeName));
    return rs.one().getLong(0);
  }

  public long getSpecificAttributeValueCount(long taskId, String nodePath, String nodeValue, String attributeName,
      String attributeValue) {
    ResultSet rs = dbService.getSession().execute(
        countSpecificAttributeValue.bind(taskId, nodePath, nodeValue, attributeName, attributeValue));
    return rs.one().getLong(0);
  }

  /**
   * Inserts the statistics for the specified attribute.
   *
   * @param taskId task identifier
   * @param attributeStatistics attribute statistics to insert
   */
  public void insertAttributeStatistics(long taskId, String nodeXpath, String nodeValue,
      AttributeStatistics attributeStatistics) {
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
   * @param taskId task identifier
   * @param nodeXpath node xpath that contains returned attributes
   * @return list of attribute statistics objects
   */
  public Set<AttributeStatistics> getAttributeStatistics(long taskId, String nodeXpath, String nodeValue, int limit) {
    return dbService.getSession().execute(selectAttributesStatement.bind(taskId, nodeXpath, nodeValue, limit))
                    .all().stream().map(this::createAttributeStatistics).collect(Collectors.toSet());
  }

  private AttributeStatistics createAttributeStatistics(Row row) {
    return new AttributeStatistics(row.getString(CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_NAME),
        row.getString(CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_VALUE),
        row.getLong(CassandraTablesAndColumnsNames.ATTRIBUTE_STATISTICS_OCCURRENCE));
  }

  public void removeAttributeStatistics(long taskId, String nodeXpath, String nodeValue) {
    BoundStatement bs = deleteAttributesStatement.bind(taskId, nodeXpath, nodeValue);
    dbService.getSession().execute(bs);
  }


}
