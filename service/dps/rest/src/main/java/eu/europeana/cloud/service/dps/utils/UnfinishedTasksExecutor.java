package eu.europeana.cloud.service.dps.utils;

import eu.europeana.cloud.common.model.dps.TaskByTaskState;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.commons.utils.DateHelper;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.services.submitters.TaskSubmitterFactory;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TaskDiagnosticInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TasksByStateDAO;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * This component will check all tasks with status TaskState.PROCESSING_BY_REST_APPLICATION
 * ({@link eu.europeana.cloud.common.model.dps.TaskState}) and start harvesting again for them.
 */
@Service
public class UnfinishedTasksExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(UnfinishedTasksExecutor.class);
  protected static final List<TaskState> RESUMABLE_TASK_STATES = Arrays.asList(TaskState.PROCESSING_BY_REST_APPLICATION,
      TaskState.DEPUBLISHING);

  private final TasksByStateDAO tasksDAO;
  private final CassandraTaskInfoDAO taskInfoDAO;
  private final TaskDiagnosticInfoDAO taskDiagnosticInfoDAO;
  private final TaskSubmitterFactory taskSubmitterFactory;
  private final String applicationIdentifier;
  private final TaskStatusUpdater taskStatusUpdater;

  UnfinishedTasksExecutor(TasksByStateDAO tasksDAO,
      CassandraTaskInfoDAO taskInfoDAO,
      TaskDiagnosticInfoDAO taskDiagnosticInfoDAO,
      TaskSubmitterFactory taskSubmitterFactory,
      String applicationIdentifier,
      TaskStatusUpdater taskStatusUpdater) {
    this.tasksDAO = tasksDAO;
    this.taskSubmitterFactory = taskSubmitterFactory;
    this.taskInfoDAO = taskInfoDAO;
    this.taskDiagnosticInfoDAO = taskDiagnosticInfoDAO;
    this.applicationIdentifier = applicationIdentifier;
    this.taskStatusUpdater = taskStatusUpdater;
  }

  @PostConstruct
  public void restartUnfinishedTasks() {
    LOGGER.info("Will restart all pending tasks");
    List<TaskByTaskState> results = findProcessingByRestTasks();
    List<TaskInfo> tasksForCurrentMachine = findTasksForCurrentMachine(results);
    resumeExecutionFor(tasksForCurrentMachine);
  }

  private List<TaskByTaskState> findProcessingByRestTasks() {
    LOGGER.info("Searching for all unfinished tasks");
    return tasksDAO.findTasksByState(RESUMABLE_TASK_STATES);
  }

  private List<TaskInfo> findTasksForCurrentMachine(List<TaskByTaskState> results) {
    LOGGER.info("Filtering tasks for current machine: {}", applicationIdentifier);
    List<TaskInfo> result = new ArrayList<>();
    for (TaskByTaskState taskInfo : results) {
      if (taskInfo.getApplicationId().equals(applicationIdentifier)) {
        taskInfoDAO.findById(taskInfo.getId()).ifPresentOrElse(result::add, () ->
            LOGGER.warn("Task with id {} not found in basic_info table. It will be ignored in resumption process.",
                taskInfo.getId()));

      }
    }
    return result;
  }

  private void resumeExecutionFor(List<TaskInfo> tasksToBeRestarted) {
    if (tasksToBeRestarted.isEmpty()) {
      LOGGER.info("No tasks to be resumed");
    } else {
      try {
        for (TaskInfo taskInfo : tasksToBeRestarted) {
          resumeTask(taskInfo);
        }
      } catch (InterruptedException ex) {
        LOGGER.error("Interruption has been encountered while execution was being resumed for task list={}", tasksToBeRestarted,
            ex);
        Thread.currentThread().interrupt();
      }
    }
  }

  private void resumeTask(TaskInfo taskInfo) throws InterruptedException {
    try {
      LOGGER.info("Resuming execution for: {}", taskInfo);
      var submitTaskParameters = prepareSubmitTaskParameters(taskInfo);
      var taskSubmitter = taskSubmitterFactory.provideTaskSubmitter(submitTaskParameters);
      taskSubmitter.submitTask(submitTaskParameters);
      taskDiagnosticInfoDAO.updateQueuedTime(taskInfo.getId(), Instant.now());
    } catch (IOException | TaskSubmissionException e) {
      LOGGER.error("Unable to resume the task", e);
      taskStatusUpdater.setTaskDropped(taskInfo.getId(), ExceptionUtils.getStackTrace(e));
    }
  }

  private SubmitTaskParameters prepareSubmitTaskParameters(TaskInfo taskInfo) throws IOException {
    var dpsTask = DpsTask.fromTaskInfo(taskInfo);

    dpsTask.addParameter(PluginParameterKeys.HARVEST_DATE, DateHelper.getISODateString(taskInfo.getSentTimestamp()));

    return SubmitTaskParameters.builder()
                               .taskInfo(TaskInfo.builder()
                                                 .sentTimestamp(taskInfo.getSentTimestamp())
                                                 .startTimestamp(new Date())
                                                 .topologyName(taskInfo.getTopologyName())
                                                 .state(TaskState.PROCESSING_BY_REST_APPLICATION)
                                                 .stateDescription(
                                                     "The task is in a pending mode, it is being processed before submission")
                                                 .definition(taskInfo.getDefinition())
                                                 .build())
                               .task(dpsTask)
                               .restarted(true).build();
  }
}
