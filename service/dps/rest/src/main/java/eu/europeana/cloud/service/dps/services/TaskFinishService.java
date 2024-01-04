package eu.europeana.cloud.service.dps.services;

import static eu.europeana.cloud.common.model.dps.TaskState.PROCESSED;
import static eu.europeana.cloud.common.model.dps.TaskState.QUEUED;
import static eu.europeana.cloud.common.model.dps.TaskState.READY_FOR_POST_PROCESSING;

import eu.europeana.cloud.common.model.dps.TaskByTaskState;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.services.postprocessors.PostProcessingService;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TasksByStateDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Class is responsible for finishing tasks that, were processed on Storm. It periodically checks number of performed records for
 * tasks. If all records are done, task state is set to PROCESSED or READY_FOR_POST_PROCESSING, for tasks that need
 * post-processing. Class operates only on tasks assigned to current server, based on application_id column in task_by_task_state
 * table.
 */
public class TaskFinishService {

  private static final Logger LOGGER = LoggerFactory.getLogger(TaskFinishService.class);
  private final String applicationId;
  private final TasksByStateDAO tasksByStateDAO;
  private final PostProcessingService postProcessingService;
  private final TaskStatusUpdater taskStatusUpdater;
  private final CassandraTaskInfoDAO taskInfoDAO;

  public TaskFinishService(
      PostProcessingService postProcessingService,
      TasksByStateDAO tasksByStateDAO,
      CassandraTaskInfoDAO taskInfoDAO,
      TaskStatusUpdater taskStatusUpdater,
      String applicationId) {

    this.postProcessingService = postProcessingService;
    this.tasksByStateDAO = tasksByStateDAO;
    this.taskInfoDAO = taskInfoDAO;
    this.taskStatusUpdater = taskStatusUpdater;
    this.applicationId = applicationId;
    LOGGER.info("Post-processing scheduler created.");
  }

  @Scheduled(fixedRate = 15_000, initialDelay = 50_000)
  public void execute() {
    findTasksInQueueState().forEach(this::handleQueuedTask);
  }

  private void handleQueuedTask(TaskByTaskState taskByTaskState) {
    TaskInfo task = taskInfoDAO.findById(taskByTaskState.getId()).orElseThrow();
    if (taskInQueueStateAlsoInTaskInfoTable(task) && task.isProcessedOnStorm()) {
      handleTaskProcessedOnStorm(taskByTaskState, task);
    } else {
      LOGGER.info("Task {} not finished yet on Storm. Status will not be changed", task.getId());
    }
  }

  private void handleTaskProcessedOnStorm(TaskByTaskState taskState, TaskInfo taskInfo) {
    boolean needsPostprocessing;
    try {
      needsPostprocessing = postProcessingService.needsPostprocessing(taskState, taskInfo);
    } catch (IOException e) {
      //It could happen when task configuration already in memory could not be deserialized, so it is
      //rather permanent problem and task should be dropped
      LOGGER.error("Unable to check if task should be post-processed id: {}!", taskInfo.getId(), e);
      taskStatusUpdater.setTaskDropped(taskState.getId(),
          "Unable to check if task should be post-processed because of: " + e.getMessage());
      return;
    }

    if (needsPostprocessing) {
      markTaskReadyForPostprocessing(taskInfo);
    } else {
      markTaskCompleted(taskInfo);
    }
  }

  private void markTaskCompleted(TaskInfo task) {
    taskStatusUpdater.setTaskCompletelyProcessed(task.getId(), PROCESSED.getDefaultMessage());
    LOGGER.info("Task completed, id: {}.", task.getId());
  }

  private void markTaskReadyForPostprocessing(TaskInfo task) {
    taskStatusUpdater.updateState(task.getId(), READY_FOR_POST_PROCESSING, READY_FOR_POST_PROCESSING.getDefaultMessage());
    LOGGER.info("Task was marked for postprocessing, id: {}.", task.getId());
  }

  private List<TaskByTaskState> findTasksInQueueState() {
    LOGGER.debug("Looking for tasks in {} state...", QUEUED);
    List<TaskByTaskState> tasks = tasksByStateDAO.findTasksByState(Collections.singletonList(QUEUED))
                                                 .stream().filter(task -> applicationId.equals(task.getApplicationId()))
                                                 .toList();
    LOGGER.debug("Found tasks in {} state : {}", QUEUED, tasks);
    return tasks;
  }

  private boolean taskInQueueStateAlsoInTaskInfoTable(TaskInfo task) {
    return TaskState.QUEUED == task.getState();
  }

}
