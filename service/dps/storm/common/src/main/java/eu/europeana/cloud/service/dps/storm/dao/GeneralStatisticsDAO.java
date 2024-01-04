package eu.europeana.cloud.service.dps.storm.dao;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyDefaultsConstants.DPS_DEFAULT_MAX_ATTEMPTS;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.annotation.Retryable;
import eu.europeana.cloud.common.model.dps.GeneralStatistics;
import eu.europeana.cloud.common.model.dps.NodeStatistics;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames;
import java.util.List;

@Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS)
public class GeneralStatisticsDAO extends CassandraDAO {

  private static GeneralStatisticsDAO instance = null;

  private PreparedStatement updateStatement;
  private PreparedStatement removeGeneralStatisticsStatement;
  private PreparedStatement searchGeneralStatistcsByTaskIdStatement;


  public GeneralStatisticsDAO(CassandraConnectionProvider dbService) {
    super(dbService);
  }

  public GeneralStatisticsDAO() {
    //needed for creating cglib proxy in RetryableMethodExecutor.createRetryProxy()
  }

  public static synchronized GeneralStatisticsDAO getInstance(CassandraConnectionProvider cassandra) {
    if (instance == null) {
      instance = RetryableMethodExecutor.createRetryProxy(new GeneralStatisticsDAO(cassandra));
    }
    return instance;
  }


  @Override
  protected void prepareStatements() {
    updateStatement = dbService.getSession().prepare(
        "UPDATE " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_TABLE
            + " SET " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_OCCURRENCE + " = "
            + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_OCCURRENCE + " + 1 "
            + "WHERE " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_TASK_ID + " = ? "
            + "AND " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_PARENT_XPATH + " = ? "
            + "AND " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_NODE_XPATH + " = ?"
    );

    removeGeneralStatisticsStatement = dbService.getSession().prepare(
        "DELETE FROM " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_TABLE
            + " WHERE " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_TASK_ID + " = ?"
    );

    searchGeneralStatistcsByTaskIdStatement = dbService.getSession().prepare(
        "SELECT *"
            + " FROM " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_TABLE
            + " WHERE " + CassandraTablesAndColumnsNames.GENERAL_STATISTICS_TASK_ID + " = ?"
    );
  }

  /**
   * It will update the counter for the specified node in general statistics table
   *
   * @param taskId task identifier
   * @param nodeStatistics node statistics object with all the necessary information
   */
  public void updateGeneralStatistics(long taskId, NodeStatistics nodeStatistics) {
    dbService.getSession().execute(updateStatement.bind(taskId, nodeStatistics.getParentXpath(), nodeStatistics.getXpath()));
  }

  public void removeGeneralStatistics(long taskId) {
    dbService.getSession().execute(removeGeneralStatisticsStatement.bind(taskId));
  }

  public List<GeneralStatistics> searchGeneralStatistics(long taskId) {
    return dbService.getSession().execute(searchGeneralStatistcsByTaskIdStatement.bind(taskId))
                    .all().stream().map(this::createGeneralStatistics).toList();
  }

  private GeneralStatistics createGeneralStatistics(Row row) {
    return GeneralStatistics.builder()
                            .parentXpath(row.getString(CassandraTablesAndColumnsNames.GENERAL_STATISTICS_PARENT_XPATH))
                            .nodeXpath(row.getString(CassandraTablesAndColumnsNames.GENERAL_STATISTICS_NODE_XPATH))
                            .occurrence(row.getLong(CassandraTablesAndColumnsNames.GENERAL_STATISTICS_OCCURRENCE))
                            .build();
  }
}
