package eu.europeana.cloud.service.dps.storm.conversion;

import com.datastax.driver.core.Row;
import eu.europeana.cloud.common.model.dps.ErrorNotification;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames;

/**
 * Class converting from DB rows or other class instances to ErrorNotification class instance
 */
public final class ErrorNotificationConverter {

  private ErrorNotificationConverter() {
  }


  /**
   * Converts database row to the {@link ErrorNotification} class instance.
   *
   * @param row database row that will be converted
   * @return {@link ErrorNotification} class instance generated based on the provided row.
   */
  public static ErrorNotification fromDBRow(Row row) {
    return ErrorNotification.builder()
                            .taskId(row.getLong(CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_TASK_ID))
                            .errorType(row.getUUID(CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_ERROR_TYPE).toString())
                            .resource(row.getString(CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_RESOURCE))
                            .additionalInformations(
                                row.getString(CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_ADDITIONAL_INFORMATIONS))
                            .errorMessage(row.getString(CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_ERROR_MESSAGE))
                            .build();
  }
}
