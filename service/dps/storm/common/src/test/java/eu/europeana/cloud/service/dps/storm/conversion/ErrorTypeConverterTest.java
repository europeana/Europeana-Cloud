package eu.europeana.cloud.service.dps.storm.conversion;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.Row;
import eu.europeana.cloud.service.dps.storm.ErrorType;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;

public class ErrorTypeConverterTest {

  public static final int COUNT = 5;
  private static final long TASK_ID = 111L;
  private static final String ERROR_TYPE = "1c71e7b0-7633-11ed-b1fe-a7fdf50126b2";
  Row row = mock(Row.class);


  @Before
  public void init() {
    when(row.getLong(CassandraTablesAndColumnsNames.ERROR_TYPES_TASK_ID)).thenReturn(TASK_ID);
    when(row.getUUID(CassandraTablesAndColumnsNames.ERROR_TYPES_ERROR_TYPE)).thenReturn(UUID.fromString(ERROR_TYPE));
    when(row.getInt(CassandraTablesAndColumnsNames.ERROR_TYPES_COUNTER)).thenReturn(COUNT);
  }

  @Test
  public void fromDBRow() {
    ErrorType errorType = ErrorTypeConverter.fromDBRow(row);
    assertEquals(TASK_ID, errorType.getTaskId());
    assertEquals(ERROR_TYPE, errorType.getUuid());
    assertEquals(COUNT, errorType.getCount());
  }
}