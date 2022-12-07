package eu.europeana.cloud.service.dps.storm.conversion;

import com.datastax.driver.core.Row;
import eu.europeana.cloud.service.dps.storm.ErrorType;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames;

/**
 * Class converting from DB rows or other class instances to ErrorType class instance
 */
public final class ErrorTypeConverter {

  private ErrorTypeConverter() {
  }


  /**
   * Converts database row to the {@link ErrorType} class instance.
   *
   * @param row database row that will be converted
   * @return {@link ErrorType} class instance generated based on the provided row.
   */
  public static ErrorType fromDBRow(Row row) {
    return ErrorType.builder()
                    .taskId(row.getLong(CassandraTablesAndColumnsNames.ERROR_TYPES_TASK_ID))
                    .uuid(row.getUUID(CassandraTablesAndColumnsNames.ERROR_TYPES_ERROR_TYPE).toString())
                    .count(row.getInt(CassandraTablesAndColumnsNames.ERROR_TYPES_COUNTER))
                    .build();
  }
}
