package eu.europeana.cloud.service.dps.storm.conversion;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.Row;
import eu.europeana.cloud.common.model.dps.ErrorNotification;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;

public class ErrorNotificationConverterTest {

  public static final String ADDITIONAL_INFORMATIONS = "some_additional_informations";
  public static final String ERROR_MESSAGE = "some_error_message";
  private static final String SOME_RESOURCE = "some_resource";
  private static final long TASK_ID = 111L;
  private static final String ERROR_TYPE = "1c71e7b0-7633-11ed-b1fe-a7fdf50126b2";
  Row row = mock(Row.class);


  @Before
  public void init() {
    when(row.getLong(CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_TASK_ID)).thenReturn(TASK_ID);
    when(row.getUUID(CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_ERROR_TYPE)).thenReturn(UUID.fromString(ERROR_TYPE));
    when(row.getString(CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_RESOURCE)).thenReturn(SOME_RESOURCE);
    when(row.getString(CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_ADDITIONAL_INFORMATIONS)).thenReturn(
        ADDITIONAL_INFORMATIONS);
    when(row.getString(CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_ERROR_MESSAGE)).thenReturn(ERROR_MESSAGE);
  }

  @Test
  public void shouldProperlyConvertFromDBRow() {
    ErrorNotification errorNotification = ErrorNotificationConverter.fromDBRow(row);
    assertEquals(TASK_ID, errorNotification.getTaskId());
    assertEquals(SOME_RESOURCE, errorNotification.getResource());
    assertEquals(ERROR_TYPE, errorNotification.getErrorType());
    assertEquals(ADDITIONAL_INFORMATIONS, errorNotification.getAdditionalInformations());
    assertEquals(ERROR_MESSAGE, errorNotification.getErrorMessage());
  }
}