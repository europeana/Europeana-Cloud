package eu.europeana.cloud.service.dps.storm.conversion;

import com.datastax.driver.core.Row;
import eu.europeana.cloud.service.dps.storm.ErrorType;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ErrorTypeConverterTest {

    public static final int COUNT = 5;
    private static final long TASK_ID = 111L;
    private static final String ERROR_TYPE = "1c71e7b0-7633-11ed-b1fe-a7fdf50126b2";
    Row row = mock(Row.class);


    @BeforeEach
    void init() {
        when(row.getLong(CassandraTablesAndColumnsNames.ERROR_TYPES_TASK_ID)).thenReturn(TASK_ID);
        when(row.getUUID(CassandraTablesAndColumnsNames.ERROR_TYPES_ERROR_TYPE)).thenReturn(UUID.fromString(ERROR_TYPE));
        when(row.getInt(CassandraTablesAndColumnsNames.ERROR_TYPES_COUNTER)).thenReturn(COUNT);
    }

    @Test
    void shouldProperlyConvertFromDBRow() {
        ErrorType errorType = ErrorTypeConverter.fromDBRow(row);
        assertEquals(TASK_ID, errorType.getTaskId());
        assertEquals(ERROR_TYPE, errorType.getUuid());
        assertEquals(COUNT, errorType.getCount());
    }
}