package eu.europeana.cloud.service.dps.storm.dao;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyDefaultsConstants.DPS_DEFAULT_MAX_ATTEMPTS;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.annotation.Retryable;
import eu.europeana.cloud.common.model.dps.Notification;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.dps.storm.conversion.NotificationConverter;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The {@link eu.europeana.cloud.common.model.dps.SubTaskInfo} DAO
 *
 * @author akrystian
 */
@Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS)
public class NotificationsDAO extends CassandraDAO {

  private static NotificationsDAO instance = null;
  public static final int BUCKET_SIZE = 10000;
  public static final String STATE_DESCRIPTION_KEY = "stateDescription";
  public static final String PROCESSING_TIME_KEY = "processingTime";
  public static final String EUROPEANA_ID_KEY = "europeanaId";

  private PreparedStatement subtaskInsertStatement;
  private PreparedStatement processedFilesCountStatement;
  private PreparedStatement removeNotificationsByTaskId;
  private PreparedStatement selectNotificationFromGivenBucketAndInGivenResourceNumRange;


  /**
   * @param dbService The service exposing the connection and session
   */
  public NotificationsDAO(CassandraConnectionProvider dbService) {
    super(dbService);
  }

  public NotificationsDAO() {
    //needed for creating cglib proxy in RetryableMethodExecutor.createRetryProxy()
  }

  public static synchronized NotificationsDAO getInstance(CassandraConnectionProvider cassandra) {
    if (instance == null) {
      instance = RetryableMethodExecutor.createRetryProxy(new NotificationsDAO(cassandra));
    }
    return instance;
  }

  /**
   * Retrieves notification records from cassandra notification table with given task_id
   *
   * @param taskId identifier of task
   * @param from minimum notification resource number value
   * @param to maximum notification resource number value
   * @param i cassandra bucket number
   * @return List of notification class instances
   */
  public List<Notification> getNotificationsFromGivenBucketAndWithinGivenResourceNumRange(long taskId, int from, int to, int i) {
    Statement selectFromNotification = selectNotificationFromGivenBucketAndInGivenResourceNumRange.bind(taskId, i, from, to);
    List<Notification> notifications = new ArrayList<>();
    dbService.getSession().execute(selectFromNotification)
             .forEach(row -> notifications.add(NotificationConverter.fromDBRow(row)));
    return notifications;
  }

  public void insert(int resourceNum, long taskId, String topologyName, String resource, String state,
      String infoTxt, Map<String, String> additionalInformation, String resultResource) {

    dbService.getSession().execute(
        subtaskInsertStatement.bind(taskId, bucketNumber(resourceNum), resourceNum, topologyName, resource, state,
            infoTxt, additionalInformation, resultResource)
    );
  }

  public BoundStatement insertNotificationStatement(Notification notification) {
    return subtaskInsertStatement.bind(
        notification.getTaskId(),
        bucketNumber(notification.getResourceNum()),
        notification.getResourceNum(),
        notification.getTopologyName(),
        notification.getResource(),
        notification.getState(),
        notification.getInfoText(),
        notification.getAdditionalInformation(),
        notification.getResultResource());
  }

  public int getProcessedFilesCount(long taskId) {
    int bucketNumber = 0;
    int filesCount = 0;
    Row row;
    do {
      ResultSet rs = dbService.getSession().execute(processedFilesCountStatement.bind(taskId, bucketNumber));
      row = rs.one();
      if (row != null) {
        filesCount = row.getInt(CassandraTablesAndColumnsNames.NOTIFICATION_RESOURCE_NUM);
        bucketNumber++;
      }
    } while (row != null);

    return filesCount;
  }

  public void removeNotifications(long taskId) {
    int lastBucket = bucketNumber(getProcessedFilesCount(taskId) - 1);
    for (int i = lastBucket; i >= 0; i--) {
      dbService.getSession().execute(removeNotificationsByTaskId.bind(taskId, i));
    }
  }

  public static int bucketNumber(int resourceNum) {
    return resourceNum / BUCKET_SIZE;
  }

  @Override
  protected void prepareStatements() {
    subtaskInsertStatement = dbService.getSession().prepare(
        String.format("insert into %s(%s, %s, %s, %s, %s, %s, %s, %s, %s) values (?,?,?,?,?,?,?,?,?)",
            CassandraTablesAndColumnsNames.NOTIFICATIONS_TABLE,
            CassandraTablesAndColumnsNames.NOTIFICATION_TASK_ID,
            CassandraTablesAndColumnsNames.NOTIFICATION_BUCKET_NUMBER,
            CassandraTablesAndColumnsNames.NOTIFICATION_RESOURCE_NUM,
            CassandraTablesAndColumnsNames.NOTIFICATION_TOPOLOGY_NAME,
            CassandraTablesAndColumnsNames.NOTIFICATION_RESOURCE,
            CassandraTablesAndColumnsNames.NOTIFICATION_STATE,
            CassandraTablesAndColumnsNames.NOTIFICATION_INFO_TEXT,
            CassandraTablesAndColumnsNames.NOTIFICATION_ADDITIONAL_INFORMATION,
            CassandraTablesAndColumnsNames.NOTIFICATION_RESULT_RESOURCE
        )
    );
    subtaskInsertStatement.setConsistencyLevel(dbService.getConsistencyLevel());

    processedFilesCountStatement = dbService.getSession().prepare(
        String.format("select %s from %s where %s = ? and %s = ? order by %s desc limit 1",
            CassandraTablesAndColumnsNames.NOTIFICATION_RESOURCE_NUM,
            CassandraTablesAndColumnsNames.NOTIFICATIONS_TABLE,
            CassandraTablesAndColumnsNames.NOTIFICATION_TASK_ID,
            CassandraTablesAndColumnsNames.NOTIFICATION_BUCKET_NUMBER,
            CassandraTablesAndColumnsNames.NOTIFICATION_RESOURCE_NUM
        )
    );
    processedFilesCountStatement.setConsistencyLevel(dbService.getConsistencyLevel());

    removeNotificationsByTaskId = dbService.getSession().prepare(
        String.format("delete from %s where %s = ? and %s = ?",
            CassandraTablesAndColumnsNames.NOTIFICATIONS_TABLE,
            CassandraTablesAndColumnsNames.NOTIFICATION_TASK_ID,
            CassandraTablesAndColumnsNames.NOTIFICATION_BUCKET_NUMBER
        )
    );

    selectNotificationFromGivenBucketAndInGivenResourceNumRange = dbService.getSession().prepare(
        "SELECT *"
            + "FROM " + CassandraTablesAndColumnsNames.NOTIFICATIONS_TABLE
            + " WHERE " + CassandraTablesAndColumnsNames.NOTIFICATION_TASK_ID + " = ?"
            + " AND " + CassandraTablesAndColumnsNames.NOTIFICATION_BUCKET_NUMBER + " = ?"
            + " AND " + CassandraTablesAndColumnsNames.NOTIFICATION_RESOURCE_NUM + " >= ?"
            + " AND " + CassandraTablesAndColumnsNames.NOTIFICATION_RESOURCE_NUM + " <= ?"
    );


  }


}
