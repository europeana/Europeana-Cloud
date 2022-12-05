package eu.europeana.cloud.service.dps.storm.conversion;

import com.datastax.driver.core.Row;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames;
/**
 * Class converting from DB rows or other class instances to TaskInfo class instance
 */
public final class TaskInfoConverter {

  private TaskInfoConverter() {
  }


    /**
     * Converts database row to the {@link TaskInfo} class instance.
     * @param row database row that will be converted
     * @return {@link TaskInfo} class instance generated based on the provided row.
     */
    public static TaskInfo fromDBRow(Row row) {
        return TaskInfo.builder()
                .id(row.getLong(CassandraTablesAndColumnsNames.TASK_INFO_TASK_ID))
                .topologyName(row.getString(CassandraTablesAndColumnsNames.TASK_INFO_TOPOLOGY_NAME))
                .expectedRecordsNumber(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_EXPECTED_RECORDS_NUMBER))
                .processedRecordsCount(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_PROCESSED_RECORDS_COUNT))
                .deletedRecordsCount(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_DELETED_RECORDS_COUNT))
                .ignoredRecordsCount(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_IGNORED_RECORDS_COUNT))
                .state(TaskState.valueOf(row.getString(CassandraTablesAndColumnsNames.TASK_INFO_STATE)))
                .stateDescription(row.getString(CassandraTablesAndColumnsNames.TASK_INFO_STATE_DESCRIPTION))
                .sentTimestamp(row.getTimestamp(CassandraTablesAndColumnsNames.TASK_INFO_SENT_TIMESTAMP))
                .startTimestamp(row.getTimestamp(CassandraTablesAndColumnsNames.TASK_INFO_START_TIMESTAMP))
                .finishTimestamp(row.getTimestamp(CassandraTablesAndColumnsNames.TASK_INFO_FINISH_TIMESTAMP))
                .processedErrorsCount(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_PROCESSED_ERRORS_COUNT))
                .deletedErrorsCount(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_DELETED_ERRORS_COUNT))
                .expectedPostProcessedRecordsNumber(
                        row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_EXPECTED_POST_PROCESSED_RECORDS_NUMBER))
                .postProcessedRecordsCount(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_POST_PROCESSED_RECORDS_COUNT))
                .definition(row.getString(CassandraTablesAndColumnsNames.TASK_INFO_DEFINITION))
                .build();
    }
}
