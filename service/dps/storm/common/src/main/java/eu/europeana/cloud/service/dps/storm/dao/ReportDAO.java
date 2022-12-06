package eu.europeana.cloud.service.dps.storm.dao;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyDefaultsConstants.DPS_DEFAULT_MAX_ATTEMPTS;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.annotation.Retryable;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames;
import java.util.UUID;

@Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS)
public class ReportDAO extends CassandraDAO {


  private static ReportDAO instance;
  private PreparedStatement selectErrorsStatement;
  private PreparedStatement selectErrorStatement;
  private PreparedStatement selectErrorCounterStatement;
  private PreparedStatement selectTaskInfo;
  private PreparedStatement selectNotification;

  public ReportDAO(CassandraConnectionProvider dbService) {
    super(dbService);
  }

  public ReportDAO() {
    //needed for creating cglib proxy in RetryableMethodExecutor.createRetryProxy()
  }

  public static synchronized ReportDAO getInstance(CassandraConnectionProvider cassandra) {
    if (instance == null) {
      instance = RetryableMethodExecutor.createRetryProxy(new ReportDAO(cassandra));
    }
    return instance;
  }

  public void prepareStatements() {
    selectErrorsStatement = dbService.getSession().prepare(
        String.format("select * from %s where %s = ?",
            CassandraTablesAndColumnsNames.ERROR_TYPES_TABLE,
            CassandraTablesAndColumnsNames.ERROR_TYPES_TASK_ID
        )
    );

    selectErrorStatement = dbService.getSession().prepare(
        String.format("select * from %s where %s = ? and %s = ? limit ?",
            CassandraTablesAndColumnsNames.ERROR_NOTIFICATIONS_TABLE,
            CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_TASK_ID,
            CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_ERROR_TYPE
        )
    );

    selectErrorCounterStatement = dbService.getSession().prepare(
        String.format("select * from %s where %s = ? and %s = ?",
            CassandraTablesAndColumnsNames.ERROR_TYPES_TABLE,
            CassandraTablesAndColumnsNames.ERROR_TYPES_TASK_ID,
            CassandraTablesAndColumnsNames.ERROR_TYPES_ERROR_TYPE
        )
    );

    selectTaskInfo = dbService.getSession().prepare(
        String.format("select * from %s where %s = ? limit 1",
            CassandraTablesAndColumnsNames.TASK_INFO_TABLE,
            CassandraTablesAndColumnsNames.TASK_INFO_TASK_ID
        )
    );

    selectNotification = dbService.getSession().prepare(
        String.format("select * from %s where %s = ? and %s = ? and %s >= ? and %s <= ?",
            CassandraTablesAndColumnsNames.NOTIFICATIONS_TABLE,
            CassandraTablesAndColumnsNames.NOTIFICATION_TASK_ID,
            CassandraTablesAndColumnsNames.NOTIFICATION_BUCKET_NUMBER,
            CassandraTablesAndColumnsNames.NOTIFICATION_RESOURCE_NUM,
            CassandraTablesAndColumnsNames.NOTIFICATION_RESOURCE_NUM
        )
    );


  }

  public Row getTaskInfoRecord(long taskId) {
    Statement selectFromTaskInfo = selectTaskInfo.bind(taskId);
    return dbService.getSession().execute(selectFromTaskInfo).one();
  }


  public ResultSet getNotification(long taskId, int from, int to, int i) {
    Statement selectFromNotification = selectNotification.bind(taskId, i, from, to);
    return dbService.getSession().execute(selectFromNotification);
  }

  public ResultSet getErrorStatements(long taskId) {
    return dbService.getSession().execute(selectErrorsStatement.bind(taskId));
  }

  public ResultSet getErrorStatement(long taskId, UUID errorTypeUid, int idsCount) {
    return dbService.getSession().execute(selectErrorStatement.bind(taskId, errorTypeUid, idsCount));
  }

  public ResultSet getErrorCounter(long taskId, UUID errorTypeUUID) {
    return dbService.getSession().execute(selectErrorCounterStatement.bind(taskId, errorTypeUUID));
  }


}

