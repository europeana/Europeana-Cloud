package eu.europeana.cloud.service.dps.storm.conversion;

import com.datastax.driver.core.Row;
import eu.europeana.cloud.common.model.dps.EngineTaskState;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames;

/**
 * Class converting from DB rows or other class instances to TaskInfo class instance
 */
public final class TaskInfoConverter {

  private TaskInfoConverter() {
  }


  /**
   * Converts database row to the {@link TaskInfo} class instance.
   *
   * @param row database row that will be converted
   * @return {@link TaskInfo} class instance generated based on the provided row.
   */
  public static TaskInfo fromDBRow(Row row) {
    return TaskInfo.builder()
            .id(row.getLong(CassandraTablesAndColumnsNames.TASK_INFO_TASK_ID))
            .topologyName(row.getString(CassandraTablesAndColumnsNames.TASK_INFO_TOPOLOGY_NAME))
            .state(EngineTaskState.valueOf(row.getString(CassandraTablesAndColumnsNames.TASK_INFO_STATE)))
            .stateDescription(row.getString(CassandraTablesAndColumnsNames.TASK_INFO_STATE_DESCRIPTION))
            .sentTimestamp(row.getTimestamp(CassandraTablesAndColumnsNames.TASK_INFO_SENT_TIMESTAMP))
            .startTimestamp(row.getTimestamp(CassandraTablesAndColumnsNames.TASK_INFO_START_TIMESTAMP))
            .finishTimestamp(row.getTimestamp(CassandraTablesAndColumnsNames.TASK_INFO_FINISH_TIMESTAMP))
            .expectedRecords(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_EXPECTED_RECORDS))
            .successRecords(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_SUCCESS_RECORDS))
            .warningRecords(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_WARNING_RECORDS))
            .failRecords(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_FAIL_RECORDS))
            .duplicateRecords(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_DUPLICATE_RECORDS))
            .unchangedRecords(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_UNCHANGED_RECORDS))
            .processedRecords(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_PROCESSED_RECORDS))
            .postProcessedRecords(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_POST_PROCESSED_RECORDS))
            .expectedPostProcessedRecords(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_EXPECTED_POST_PROCESSED_RECORDS))
            .expectedDepublishRecords(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_EXPECTED_DEPUBLISH_RECORDS))
            .successDepublishRecords(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_SUCCESS_DEPUBLISH_RECORDS))
            .failDepublishRecords(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_FAIL_DEPUBLISH_RECORDS))
            .processedDepublishRecords(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_PROCESSED_DEPUBLISH_RECORDS))
            .definition(row.getString(CassandraTablesAndColumnsNames.TASK_INFO_DEFINITION))
            .build();
  }

    /**
     * Converts database row to the {@link TaskInfo} class instance.
     *
     * @param row database row that will be converted
     * @return {@link TaskInfo} class instance generated based on the provided row.
     */
    public static TaskInfo fromDBRowBackwardCompatible(Row row) {
        return TaskInfo.builder()
                .id(row.getLong(CassandraTablesAndColumnsNames.TASK_INFO_TASK_ID))
                .topologyName(row.getString(CassandraTablesAndColumnsNames.TASK_INFO_TOPOLOGY_NAME))
                .state(EngineTaskState.valueOf(row.getString(CassandraTablesAndColumnsNames.TASK_INFO_STATE)))
                .stateDescription(row.getString(CassandraTablesAndColumnsNames.TASK_INFO_STATE_DESCRIPTION))
                .sentTimestamp(row.getTimestamp(CassandraTablesAndColumnsNames.TASK_INFO_SENT_TIMESTAMP))
                .startTimestamp(row.getTimestamp(CassandraTablesAndColumnsNames.TASK_INFO_START_TIMESTAMP))
                .finishTimestamp(row.getTimestamp(CassandraTablesAndColumnsNames.TASK_INFO_FINISH_TIMESTAMP))
                .definition(row.getString(CassandraTablesAndColumnsNames.TASK_INFO_DEFINITION))
                .expectedRecords(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_EXPECTED_RECORDS_NUMBER)
                        - row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_DELETED_RECORDS_COUNT))
                .successRecords(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_PROCESSED_RECORDS_COUNT)
                        - row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_PROCESSED_ERRORS_COUNT))
                .warningRecords(-1)
                .failRecords(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_PROCESSED_ERRORS_COUNT))
                .duplicateRecords(-1)
                .unchangedRecords(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_IGNORED_RECORDS_COUNT))
                .processedRecords(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_PROCESSED_RECORDS_COUNT)
                        + row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_IGNORED_RECORDS_COUNT))
                .postProcessedRecords(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_POST_PROCESSED_RECORDS_COUNT))
                .expectedPostProcessedRecords(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_EXPECTED_POST_PROCESSED_RECORDS_NUMBER))
                .expectedDepublishRecords(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_DELETED_RECORDS_COUNT)
                        + getValue(row, CassandraTablesAndColumnsNames.TASK_INFO_EXPECTED_POST_PROCESSED_RECORDS_NUMBER))
                .successDepublishRecords(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_DELETED_RECORDS_COUNT)
                        - row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_DELETED_ERRORS_COUNT)
                        + row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_POST_PROCESSED_RECORDS_COUNT))
                .failDepublishRecords(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_DELETED_ERRORS_COUNT))
                .processedDepublishRecords(row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_DELETED_RECORDS_COUNT)
                        + row.getInt(CassandraTablesAndColumnsNames.TASK_INFO_POST_PROCESSED_RECORDS_COUNT))
                .build();
    }


    private static int getValue(Row row, String columnName) {
        return Math.max(0, row.getInt(columnName));
    }
}

