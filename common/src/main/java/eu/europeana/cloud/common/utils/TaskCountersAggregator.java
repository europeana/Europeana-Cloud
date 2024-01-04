package eu.europeana.cloud.common.utils;

import eu.europeana.cloud.common.model.dps.TaskInfo;

public final class TaskCountersAggregator {

  private TaskCountersAggregator() {
  }

  /**
   * Calculates the number of records that are ready to be processed by the next topology in the workflow. This value will be used
   * as 'expected_records_number' in the next step in the workflow.
   *
   * @param taskInfo {@link TaskInfo} instance that will be used as a source of raw counters
   * @return 'expected_records_number' for the next topology
   */
  public static int recordsDesignedForTheFurtherProcessing(TaskInfo taskInfo) {
    return taskInfo.getProcessedRecordsCount() + taskInfo.getDeletedRecordsCount() + taskInfo.getPostProcessedRecordsCount();
  }
}
