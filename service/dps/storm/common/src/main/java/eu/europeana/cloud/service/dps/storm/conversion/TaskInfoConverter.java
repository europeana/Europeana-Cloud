package eu.europeana.cloud.service.dps.storm.conversion;

import com.datastax.driver.core.Row;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames;

public class TaskInfoConverter {

    /**
     * Converts database row to the {@link TaskInfo} class instance.
     * @param row database row that will be converted
     * @return {@link TaskInfo} class instance generated based on the provided row.
     */
    public static TaskInfo fromDBRow(Row row) {
        return TaskInfo.builder()
                .id(row.getLong(CassandraTablesAndColumnsNames.TASK_INFO_TASK_ID))
                .topologyName(row.getString(CassandraTablesAndColumnsNames.TASK_INFO_TOPOLOGY_NAME))
                .state(TaskState.valueOf(row.getString(CassandraTablesAndColumnsNames.TASK_INFO_STATE)))
                .stateDescription(row.getString(CassandraTablesAndColumnsNames.TASK_INFO_STATE_DESCRIPTION))
                .sentTimestamp(row.getTimestamp(CassandraTablesAndColumnsNames.TASK_INFO_SENT_TIMESTAMP))
                .startTimestamp(row.getTimestamp(CassandraTablesAndColumnsNames.TASK_INFO_START_TIMESTAMP))
                .finishTimestamp(row.getTimestamp(CassandraTablesAndColumnsNames.TASK_INFO_FINISH_TIMESTAMP))
                .expectedRecordsNumber(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_EXPECTED_RECORDS_NUMBER))
                .processedRecordsCount(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_PROCESSED_RECORDS_COUNT))
                .definition(row.getString(CassandraTablesAndColumnsNames.TASK_INFO_DEFINITION))
                .processedErrorsCount(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_PROCESSED_ERRORS_COUNT))
                .build();
    }
}
