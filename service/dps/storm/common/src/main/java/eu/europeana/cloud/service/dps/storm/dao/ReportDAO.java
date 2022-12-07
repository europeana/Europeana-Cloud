package eu.europeana.cloud.service.dps.storm.dao;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyDefaultsConstants.DPS_DEFAULT_MAX_ATTEMPTS;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.annotation.Retryable;
import eu.europeana.cloud.common.model.dps.ErrorNotification;
import eu.europeana.cloud.common.model.dps.Notification;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.dps.storm.ErrorType;
import eu.europeana.cloud.service.dps.storm.conversion.ErrorNotificationConverter;
import eu.europeana.cloud.service.dps.storm.conversion.ErrorTypeConverter;
import eu.europeana.cloud.service.dps.storm.conversion.NotificationConverter;
import eu.europeana.cloud.service.dps.storm.conversion.TaskInfoConverter;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS)
public class ReportDAO extends CassandraDAO {


  private static ReportDAO instance;
  private PreparedStatement selectErrorTypesStatement;
  private PreparedStatement selectErrorNotificationsStatement;
  private PreparedStatement selectErrorTypeStatement;
  private PreparedStatement selectTaskInfo;
  private PreparedStatement selectNotifications;

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
    selectErrorTypesStatement = dbService.getSession().prepare(
        String.format("select * from %s where %s = ?",
            CassandraTablesAndColumnsNames.ERROR_TYPES_TABLE,
            CassandraTablesAndColumnsNames.ERROR_TYPES_TASK_ID
        )
    );

    selectErrorNotificationsStatement = dbService.getSession().prepare(
        String.format("select * from %s where %s = ? and %s = ? limit ?",
            CassandraTablesAndColumnsNames.ERROR_NOTIFICATIONS_TABLE,
            CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_TASK_ID,
            CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_ERROR_TYPE
        )
    );

    selectErrorTypeStatement = dbService.getSession().prepare(
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

    selectNotifications = dbService.getSession().prepare(
        String.format("select * from %s where %s = ? and %s = ? and %s >= ? and %s <= ?",
            CassandraTablesAndColumnsNames.NOTIFICATIONS_TABLE,
            CassandraTablesAndColumnsNames.NOTIFICATION_TASK_ID,
            CassandraTablesAndColumnsNames.NOTIFICATION_BUCKET_NUMBER,
            CassandraTablesAndColumnsNames.NOTIFICATION_RESOURCE_NUM,
            CassandraTablesAndColumnsNames.NOTIFICATION_RESOURCE_NUM
        )
    );


  }

  public TaskInfo getTaskInfoRecord(long taskId) {
    Statement selectFromTaskInfo = selectTaskInfo.bind(taskId);
    ResultSet rs = dbService.getSession().execute(selectFromTaskInfo);
    return (rs.iterator().hasNext()) ? TaskInfoConverter.fromDBRow(rs.one()) : null;
  }


  public List<Notification> getNotifications(long taskId, int from, int to, int i) {
    Statement selectFromNotification = selectNotifications.bind(taskId, i, from, to);
    List<Notification> notifications = new ArrayList<>();
    dbService.getSession().execute(selectFromNotification)
             .forEach(row -> notifications.add(NotificationConverter.fromDBRow(row)));
    return notifications;
  }


  public List<ErrorType> getErrorTypes(long taskId) {
    List<ErrorType> errorTypes = new ArrayList<>();
    dbService.getSession().execute(selectErrorTypesStatement.bind(taskId)).forEach(
        row -> errorTypes.add(ErrorTypeConverter.fromDBRow(row))
    );
    return errorTypes;
  }

  public List<ErrorNotification> getErrorNotifications(long taskId, UUID errorTypeUid, int idsCount) {
    List<ErrorNotification> errorNotifications = new ArrayList<>();
    dbService.getSession().execute(selectErrorNotificationsStatement.bind(taskId, errorTypeUid, idsCount)).forEach(row ->
        errorNotifications.add(ErrorNotificationConverter.fromDBRow(row))
    );
    return errorNotifications;
  }

  public ErrorType getErrorType(long taskId, UUID errorTypeUUID) {
    ResultSet rs = dbService.getSession().execute(selectErrorTypeStatement.bind(taskId, errorTypeUUID));
    return (rs.iterator().hasNext()) ? ErrorTypeConverter.fromDBRow(rs.one()) : null;
  }


}

