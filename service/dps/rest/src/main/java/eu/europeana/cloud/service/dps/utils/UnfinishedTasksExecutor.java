package eu.europeana.cloud.service.dps.utils;

import eu.europeana.cloud.common.model.dps.TaskByTaskState;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.commons.utils.DateHelper;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.services.SubmitTaskService;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TasksByStateDAO;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import jakarta.annotation.PostConstruct;
import java.io.IOException;
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
  protected static final List<TaskState> RESUMABLE_TASK_STATES = Arrays.asList(TaskState.PROCESSING_BY_REST_APPLICATION);

  private final TasksByStateDAO tasksDAO;
  private final CassandraTaskInfoDAO taskInfoDAO;
  private final String applicationIdentifier;
  private final TaskStatusUpdater taskStatusUpdater;
  private final SubmitTaskService submitTaskService;

  UnfinishedTasksExecutor(TasksByStateDAO tasksDAO,
      CassandraTaskInfoDAO taskInfoDAO,
      String applicationIdentifier,
      TaskStatusUpdater taskStatusUpdater,
      SubmitTaskService submitTaskService) {
    this.tasksDAO = tasksDAO;
    this.taskInfoDAO = taskInfoDAO;
    this.applicationIdentifier = applicationIdentifier;
    this.taskStatusUpdater = taskStatusUpdater;
    this.submitTaskService = submitTaskService;
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
      submitTaskService.submitTask(submitTaskParameters);
    } catch (IOException e) {
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
                                                 .expectedRecordsNumber(taskInfo.getExpectedRecordsNumber())
                                                 .build())
                               .task(dpsTask)
                               .restarted(true).build();
  }
}
