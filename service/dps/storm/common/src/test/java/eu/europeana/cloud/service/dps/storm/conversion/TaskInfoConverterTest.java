package eu.europeana.cloud.service.dps.storm.conversion;

import com.datastax.driver.core.Row;
import eu.europeana.cloud.common.model.dps.EngineTaskState;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TaskInfoConverterTest {

    public static final String TOPOLOGY_NAME = "some_topology_name";
    public static final int EXPECTED_RECORDS = 1;
    public static final int PROCESSED_RECORDS = 2;
    public static final int SUCCESS_RECORDS = 3;
    public static final int WARNING_RECORDS = 4;
    public static final int FAIL_RECORDS = 5;
    public static final int DUPLICATE_RECORDS = 6;
    public static final int UNCHANGED_RECORDS = 7;
    public static final int POST_PROCESSED_RECORDS = 8;
    public static final int PROCESSED_DEPUBLISH_RECORDS = 9;
    public static final int EXPECTED_DEPUBLISH_RECORDS = 10;
    public static final int SUCCESS_DEPUBLISH_RECORDS = 11;
    public static final int FAIL_DEPUBLISH_RECORDS = 12;
    public static final int EXPECTED_POST_PROCESSED = 13;
    public static final EngineTaskState STATE = EngineTaskState.QUEUED;
    public static final String DESCRIPTION = "some_state_description";
    public static final Date SENT = Date.from(Instant.now());
    public static final Date START = Date.from(Instant.now());
    public static final Date FINISH = Date.from(Instant.now());
    public static final String DEFINITION = "some_definition";
    private static final long TASK_ID = 111L;
    Row row = mock(Row.class);


    @BeforeEach
    void init() {
        when(row.getLong(CassandraTablesAndColumnsNames.TASK_INFO_TASK_ID))
                .thenReturn(TASK_ID);

        when(row.getString(CassandraTablesAndColumnsNames.TASK_INFO_TOPOLOGY_NAME))
                .thenReturn(TOPOLOGY_NAME);

        when(row.getString(CassandraTablesAndColumnsNames.TASK_INFO_STATE))
                .thenReturn(STATE.toString());

        when(row.getString(CassandraTablesAndColumnsNames.TASK_INFO_STATE_DESCRIPTION))
                .thenReturn(DESCRIPTION);

        when(row.getTimestamp(CassandraTablesAndColumnsNames.TASK_INFO_SENT_TIMESTAMP))
                .thenReturn(SENT);

        when(row.getTimestamp(CassandraTablesAndColumnsNames.TASK_INFO_START_TIMESTAMP))
                .thenReturn(START);

        when(row.getTimestamp(CassandraTablesAndColumnsNames.TASK_INFO_FINISH_TIMESTAMP))
                .thenReturn(FINISH);

        when(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_EXPECTED_RECORDS))
                .thenReturn(EXPECTED_RECORDS);

        when(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_PROCESSED_RECORDS))
                .thenReturn(PROCESSED_RECORDS);

        when(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_SUCCESS_RECORDS))
                .thenReturn(SUCCESS_RECORDS);

        when(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_WARNING_RECORDS))
                .thenReturn(WARNING_RECORDS);

        when(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_FAIL_RECORDS))
                .thenReturn(FAIL_RECORDS);

        when(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_DUPLICATE_RECORDS))
                .thenReturn(DUPLICATE_RECORDS);

        when(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_UNCHANGED_RECORDS))
                .thenReturn(UNCHANGED_RECORDS);


        when(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_EXPECTED_POST_PROCESSED_RECORDS))
                .thenReturn(EXPECTED_POST_PROCESSED);

        when(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_POST_PROCESSED_RECORDS))
                .thenReturn(POST_PROCESSED_RECORDS);

        when(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_PROCESSED_DEPUBLISH_RECORDS))
                .thenReturn(PROCESSED_DEPUBLISH_RECORDS);

        when(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_EXPECTED_DEPUBLISH_RECORDS))
                .thenReturn(EXPECTED_DEPUBLISH_RECORDS);

        when(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_SUCCESS_DEPUBLISH_RECORDS))
                .thenReturn(SUCCESS_DEPUBLISH_RECORDS);

        when(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_FAIL_DEPUBLISH_RECORDS))
                .thenReturn(FAIL_DEPUBLISH_RECORDS);

        when(row.getString(CassandraTablesAndColumnsNames.TASK_INFO_DEFINITION))
                .thenReturn(DEFINITION);
    }
    @Test
    void shouldProperlyConvertFromDBRow() {

        TaskInfo taskInfo = TaskInfoConverter.fromDBRow(row);

        assertEquals(TASK_ID, taskInfo.getId());
        assertEquals(TOPOLOGY_NAME, taskInfo.getTopologyName());

        assertEquals(EXPECTED_RECORDS, taskInfo.getExpectedRecords());
        assertEquals(PROCESSED_RECORDS, taskInfo.getProcessedRecords());

        assertEquals(SUCCESS_RECORDS, taskInfo.getSuccessRecords());
        assertEquals(WARNING_RECORDS, taskInfo.getWarningRecords());
        assertEquals(FAIL_RECORDS, taskInfo.getFailRecords());

        assertEquals(DUPLICATE_RECORDS, taskInfo.getDuplicateRecords());
        assertEquals(UNCHANGED_RECORDS, taskInfo.getUnchangedRecords());

        assertEquals(PROCESSED_RECORDS, taskInfo.getProcessedRecords());

        assertEquals(STATE, taskInfo.getEngineTaskState());
        assertEquals(DESCRIPTION, taskInfo.getEngineTaskStateInfo());

        assertEquals(SENT, taskInfo.getSentTimestamp());
        assertEquals(START, taskInfo.getStartTimestamp());
        assertEquals(FINISH, taskInfo.getFinishTimestamp());

        assertEquals(EXPECTED_POST_PROCESSED, taskInfo.getExpectedPostProcessedRecords());
        assertEquals(POST_PROCESSED_RECORDS, taskInfo.getPostProcessedRecords());

        assertEquals(PROCESSED_DEPUBLISH_RECORDS, taskInfo.getProcessedDepublishRecords());
        assertEquals(EXPECTED_DEPUBLISH_RECORDS, taskInfo.getExpectedDepublishRecords());
        assertEquals(SUCCESS_DEPUBLISH_RECORDS, taskInfo.getSuccessDepublishRecords());
        assertEquals(FAIL_DEPUBLISH_RECORDS, taskInfo.getFailDepublishRecords());

        assertEquals(DEFINITION, taskInfo.getDefinition());
    }
}