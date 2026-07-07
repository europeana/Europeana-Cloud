package eu.europeana.cloud.service.dps.services.postprocessors;

import eu.europeana.cloud.common.model.dps.TaskByTaskState;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.exception.TaskInfoDoesNotExistException;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TaskDiagnosticInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskDroppedException;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import java.io.IOException;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;

public class PostProcessingService {

  private static final Logger LOGGER = LoggerFactory.getLogger(PostProcessingService.class);
  private static final String MESSAGE_SUCCESSFULLY_POST_PROCESSED = "Successfully post processed task with id={}";
  private static final String MESSAGE_FAILED_POST_PROCESSED = "Could not post process task with id={}";

  private final CassandraTaskInfoDAO taskInfoDAO;

  private final TaskDiagnosticInfoDAO taskDiagnosticInfoDAO;

  private final TaskStatusUpdater taskStatusUpdater;

  private final PostProcessorFactory postProcessorFactory;

  public PostProcessingService(PostProcessorFactory postProcessorFactory,
      CassandraTaskInfoDAO taskInfoDAO,
      TaskDiagnosticInfoDAO taskDiagnosticInfoDAO,
      TaskStatusUpdater taskStatusUpdater) {

    this.postProcessorFactory = postProcessorFactory;
    this.taskInfoDAO = taskInfoDAO;
    this.taskDiagnosticInfoDAO = taskDiagnosticInfoDAO;
    this.taskStatusUpdater = taskStatusUpdater;
  }

  @Async("postProcessingExecutor")
  public void postProcess(TaskByTaskState taskByTaskState) {
    try {
      LOGGER.debug("Starting postprocessing for task: {} on thread {}.", taskByTaskState.getId(),
          Thread.currentThread().getName());
      TaskPostProcessor postProcessor = postProcessorFactory.getPostProcessor(taskByTaskState);
      var taskInfo = loadTask(taskByTaskState);
      DpsTask task = DpsTask.fromTaskInfo(taskInfo);
      taskDiagnosticInfoDAO.updatePostprocessingStartTime(taskByTaskState.getId(), Instant.now());
      postProcessor.execute(taskInfo, task);
      LOGGER.info(MESSAGE_SUCCESSFULLY_POST_PROCESSED, taskByTaskState.getId());
    } catch (TaskDroppedException exception) {
      LOGGER.info("The task: {} could not be post-processed because it was dropped", exception.getTaskId(), exception);
    } catch (IOException | TaskInfoDoesNotExistException | PostProcessingException exception) {
      LOGGER.error(MESSAGE_FAILED_POST_PROCESSED, taskByTaskState.getId(), exception);
      taskStatusUpdater.setTaskDropped(taskByTaskState.getId(), exception.getMessage());
    }
  }

  private TaskInfo loadTask(TaskByTaskState taskByTaskState) throws TaskInfoDoesNotExistException {
    return taskInfoDAO.findById(taskByTaskState.getId()).orElseThrow(TaskInfoDoesNotExistException::new);
  }

  public boolean needsPostprocessing(TaskByTaskState taskByTaskState, TaskInfo taskInfo) throws IOException {
    DpsTask task = DpsTask.fromTaskInfo(taskInfo);
    return postProcessorFactory.findPostProcessor(taskByTaskState)
                               .map(postProcessor -> postProcessor.needsPostProcessing(task))
                               .orElse(Boolean.FALSE);
  }
}
