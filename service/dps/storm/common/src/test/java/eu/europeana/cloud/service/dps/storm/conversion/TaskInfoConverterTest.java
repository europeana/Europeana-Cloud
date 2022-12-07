package eu.europeana.cloud.service.dps.storm.conversion;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.Row;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames;
import java.time.Instant;
import java.util.Date;
import org.junit.Before;
import org.junit.Test;

public class TaskInfoConverterTest {

  public static final String TOPOLOGY_NAME = "some_topology_name";
  public static final int EXPECTED_RECORDS_NUMBER = 1;
  public static final int PROCESSED_RECORDS_COUNT = 2;
  public static final int DELETED_RECORDS_COUNT = 3;
  public static final int IGNORED_RECORDS_COUNT = 4;
  public static final TaskState STATE = TaskState.QUEUED;
  public static final String DESCRIPTION = "some_state_description";
  public static final Date SENT = Date.from(Instant.now());
  public static final Date START = Date.from(Instant.now());
  public static final Date FINISH = Date.from(Instant.now());
  public static final int PROCESSED_ERRORS_COUNT = 5;
  public static final int DELETED_ERRORS_COUNT = 6;
  public static final int EXPECTED_POST_PROCESSED_RECORD_NUMBER = 7;
  public static final int POST_PROCESSED_RECORDS_COUNT = 8;
  public static final String DEFINITION = "some_definition";
  private static final long TASK_ID = 111L;
  Row row = mock(Row.class);


  @Before
  public void init() {
    when(row.getLong(CassandraTablesAndColumnsNames.TASK_INFO_TASK_ID)).thenReturn(TASK_ID);
    when(row.getString(CassandraTablesAndColumnsNames.TASK_INFO_TOPOLOGY_NAME)).thenReturn(TOPOLOGY_NAME);
    when(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_EXPECTED_RECORDS_NUMBER)).thenReturn(EXPECTED_RECORDS_NUMBER);
    when(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_PROCESSED_RECORDS_COUNT)).thenReturn(PROCESSED_RECORDS_COUNT);
    when(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_DELETED_RECORDS_COUNT)).thenReturn(DELETED_RECORDS_COUNT);
    when(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_IGNORED_RECORDS_COUNT)).thenReturn(IGNORED_RECORDS_COUNT);
    when(row.getString(CassandraTablesAndColumnsNames.TASK_INFO_STATE)).thenReturn(STATE.toString());
    when(row.getString(CassandraTablesAndColumnsNames.TASK_INFO_STATE_DESCRIPTION)).thenReturn(DESCRIPTION);
    when(row.getTimestamp(CassandraTablesAndColumnsNames.TASK_INFO_SENT_TIMESTAMP)).thenReturn(SENT);
    when(row.getTimestamp(CassandraTablesAndColumnsNames.TASK_INFO_START_TIMESTAMP)).thenReturn(START);
    when(row.getTimestamp(CassandraTablesAndColumnsNames.TASK_INFO_FINISH_TIMESTAMP)).thenReturn(FINISH);
    when(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_PROCESSED_ERRORS_COUNT)).thenReturn(PROCESSED_ERRORS_COUNT);
    when(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_DELETED_ERRORS_COUNT)).thenReturn(DELETED_ERRORS_COUNT);
    when(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_EXPECTED_POST_PROCESSED_RECORDS_NUMBER)).thenReturn(
        EXPECTED_POST_PROCESSED_RECORD_NUMBER);
    when(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_POST_PROCESSED_RECORDS_COUNT)).thenReturn(
        POST_PROCESSED_RECORDS_COUNT);
    when(row.getString(CassandraTablesAndColumnsNames.TASK_INFO_DEFINITION)).thenReturn(DEFINITION);
  }

  @Test
  public void fromDBRow() {
    TaskInfo taskInfo = TaskInfoConverter.fromDBRow(row);
    assertEquals(TASK_ID, taskInfo.getId());
    assertEquals(TOPOLOGY_NAME, taskInfo.getTopologyName());
    assertEquals(EXPECTED_RECORDS_NUMBER, taskInfo.getExpectedRecordsNumber());
    assertEquals(PROCESSED_RECORDS_COUNT, taskInfo.getProcessedRecordsCount());
    assertEquals(DELETED_RECORDS_COUNT, taskInfo.getDeletedRecordsCount());
    assertEquals(IGNORED_RECORDS_COUNT, taskInfo.getIgnoredRecordsCount());
    assertEquals(STATE, taskInfo.getState());
    assertEquals(DESCRIPTION, taskInfo.getStateDescription());
    assertEquals(SENT, taskInfo.getSentTimestamp());
    assertEquals(START, taskInfo.getStartTimestamp());
    assertEquals(FINISH, taskInfo.getFinishTimestamp());
    assertEquals(PROCESSED_ERRORS_COUNT, taskInfo.getProcessedErrorsCount());
    assertEquals(DELETED_ERRORS_COUNT, taskInfo.getDeletedErrorsCount());
    assertEquals(EXPECTED_POST_PROCESSED_RECORD_NUMBER, taskInfo.getExpectedPostProcessedRecordsNumber());
    assertEquals(POST_PROCESSED_RECORDS_COUNT, taskInfo.getPostProcessedRecordsCount());
    assertEquals(DEFINITION, taskInfo.getDefinition());
  }
}