package eu.europeana.cloud.service.dps.services.postprocessors;

import static eu.europeana.cloud.common.log.AttributePassingUtils.runWithTaskIdLogAttr;
import static eu.europeana.cloud.common.model.dps.TaskState.IN_POST_PROCESSING;
import static eu.europeana.cloud.common.model.dps.TaskState.READY_FOR_POST_PROCESSING;

import eu.europeana.cloud.common.model.dps.TaskByTaskState;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.storm.dao.TasksByStateDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import jakarta.annotation.PostConstruct;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.task.TaskRejectedException;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Component responsible for executing postprocessing for the tasks. It uses Scheduler for that.
 */
public class PostProcessingScheduler {

  private static final Logger LOGGER = LoggerFactory.getLogger(PostProcessingScheduler.class);
  private final String applicationId;

  private final TasksByStateDAO tasksByStateDAO;

  private final PostProcessingService postProcessingService;

  private final TaskStatusUpdater taskStatusUpdater;

  public PostProcessingScheduler(
      PostProcessingService postProcessingService,
      TasksByStateDAO tasksByStateDAO,
      TaskStatusUpdater taskStatusUpdater,
      String applicationId) {

    this.postProcessingService = postProcessingService;
    this.tasksByStateDAO = tasksByStateDAO;
    this.taskStatusUpdater = taskStatusUpdater;
    this.applicationId = applicationId;
    LOGGER.info("Post-processing scheduler created.");
  }

  @PostConstruct
  public void init() {
    LOGGER.debug("Initializing Postprocessing scheduler");
    resetTasksState();
  }


  @Scheduled(fixedRate = 30_000, initialDelay = 60_000)
  public void execute() {
    findTasksIn(List.of(READY_FOR_POST_PROCESSING)).forEach(
        task -> runWithTaskIdLogAttr(task.getId(), () -> executePostProcessingAsync(task)));
  }

  private void executePostProcessingAsync(TaskByTaskState taskByTaskState) {
    try {
      postProcessingService.postProcess(taskByTaskState);
      LOGGER.info("Task was sent to post-processing, id: {}.", taskByTaskState.getId());
    } catch (TaskRejectedException e) {
      LOGGER.error("Unable to submit the task id: {} for postprocessing", taskByTaskState.getId(), e);
      taskStatusUpdater.setTaskDropped(taskByTaskState.getId(),
          "Unable to postprocess the task because of: " + e.getMessage());
    }
  }

  public void resetTasksState() {
    LOGGER.info("Resetting state of all pending tasks to {}", IN_POST_PROCESSING);
    findTasksIn(List.of(IN_POST_PROCESSING)).forEach(this::resetTaskState);
  }

  private List<TaskByTaskState> findTasksIn(List<TaskState> states) {
    LOGGER.debug("Looking for tasks in {} state(s)...", states);
    List<TaskByTaskState> tasks = tasksByStateDAO.findTasksByState(states)
                                                 .stream().filter(task -> applicationId.equals(task.getApplicationId()))
                                                 .toList();
    LOGGER.debug("Found tasks in {} state(s) : {}", states, tasks);
    return tasks;
  }

  private void resetTaskState(TaskByTaskState taskByTaskState) {
    LOGGER.info("Resetting state of task {} to {}", taskByTaskState.getId(), IN_POST_PROCESSING);
    taskStatusUpdater.updateState(taskByTaskState.getId(), READY_FOR_POST_PROCESSING, "Resetting task state");
  }


}
