package eu.europeana.cloud.service.dps.storm.conversion;

import com.datastax.driver.core.Row;
import eu.europeana.cloud.common.model.dps.Notification;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames;

/**
 * Class converting from DB rows or other class instances to Notification class instance
 */
public final class NotificationConverter {

  private NotificationConverter() {
  }


  /**
   * Converts database row to the {@link Notification} class instance.
   *
   * @param row database row that will be converted
   * @return {@link Notification} class instance generated based on the provided row.
   */
  public static Notification fromDBRow(Row row) {
    return Notification.builder()
                       .taskId(row.getLong(CassandraTablesAndColumnsNames.NOTIFICATION_TASK_ID))
                       .bucketNumber(row.getInt(CassandraTablesAndColumnsNames.NOTIFICATION_BUCKET_NUMBER))
                       .resourceNum(row.getInt(CassandraTablesAndColumnsNames.NOTIFICATION_RESOURCE_NUM))
                       .additionalInformation(
                           row.getMap(CassandraTablesAndColumnsNames.NOTIFICATION_ADDITIONAL_INFORMATION, String.class,
                               String.class))
                       .infoText(row.getString(CassandraTablesAndColumnsNames.NOTIFICATION_INFO_TEXT))
                       .resource(row.getString(CassandraTablesAndColumnsNames.NOTIFICATION_RESOURCE))
                       .resultResource(row.getString(CassandraTablesAndColumnsNames.NOTIFICATION_RESULT_RESOURCE))
                       .state(row.getString(CassandraTablesAndColumnsNames.NOTIFICATION_STATE))
                       .topologyName(row.getString(CassandraTablesAndColumnsNames.NOTIFICATION_TOPOLOGY_NAME))
                       .build();
  }
}
