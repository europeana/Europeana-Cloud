package eu.europeana.cloud.service.dps.services.postprocessors;

import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public abstract class TaskPostProcessor {

  protected final TaskStatusChecker taskStatusChecker;
  protected final TaskStatusUpdater taskStatusUpdater;
  protected final HarvestedRecordsDAO harvestedRecordsDAO;

  protected TaskPostProcessor(TaskStatusChecker taskStatusChecker,
      TaskStatusUpdater taskStatusUpdater,
      HarvestedRecordsDAO harvestedRecordsDAO) {
    this.taskStatusChecker = taskStatusChecker;
    this.taskStatusUpdater = taskStatusUpdater;
    this.harvestedRecordsDAO = harvestedRecordsDAO;
  }

  /**
   * Executes post-processing activity for the provided task
   */
  public void execute(TaskInfo taskInfo, DpsTask dpsTask) {
    taskStatusChecker.checkNotDropped(dpsTask);
    executePostprocessing(taskInfo, dpsTask);
  }

  protected boolean taskIsDropped(DpsTask dpsTask) {
    return taskStatusChecker.hasDroppedStatus(dpsTask.getTaskId());
  }

  abstract void executePostprocessing(TaskInfo taskInfo, DpsTask dpsTask);

  /**
   * Returns set of names of processed topologies
   */
  abstract Set<String> getProcessedTopologies();

  abstract boolean needsPostProcessing(DpsTask task);

}
