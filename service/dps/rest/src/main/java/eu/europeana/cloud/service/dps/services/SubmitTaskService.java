package eu.europeana.cloud.service.dps.services;

import eu.europeana.cloud.service.dps.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.services.submitters.TaskSubmitter;
import eu.europeana.cloud.service.dps.services.submitters.TaskSubmitterFactory;
import eu.europeana.cloud.service.dps.storm.dao.TaskDiagnosticInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import java.time.Instant;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Service
public class SubmitTaskService {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubmitTaskService.class);

  private final TaskSubmitterFactory taskSubmitterFactory;
  private final TaskStatusUpdater taskStatusUpdater;
  private final TaskDiagnosticInfoDAO taskDiagnosticInfoDAO;

  public SubmitTaskService(TaskSubmitterFactory taskSubmitterFactory, TaskStatusUpdater taskStatusUpdater,
      TaskDiagnosticInfoDAO taskDiagnosticInfoDAO) {
    this.taskStatusUpdater = taskStatusUpdater;
    this.taskSubmitterFactory = taskSubmitterFactory;
    this.taskDiagnosticInfoDAO = taskDiagnosticInfoDAO;
  }

  @Async
  public void submitTask(SubmitTaskParameters parameters) {
    try {
      TaskSubmitter taskSubmitter = taskSubmitterFactory.provideTaskSubmitter(parameters);
      taskSubmitter.submitTask(parameters);
      taskDiagnosticInfoDAO.updateQueuedTime(parameters.getTask().getTaskId(), Instant.now());
    } catch (TaskSubmissionException e) {
      LOGGER.error("Task submission failed: {}", e.getMessage(), e);
      taskStatusUpdater.setTaskDropped(parameters.getTask().getTaskId(), prepareExceptionMessage(e));
    } catch (InterruptedException e) {
      LOGGER.error("Task submission failed due to interruption exception:", e);
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      String fullStacktrace = ExceptionUtils.getStackTrace(e);
      LOGGER.error("Task submission failed: {}", fullStacktrace);
      taskStatusUpdater.setTaskDropped(parameters.getTask().getTaskId(), fullStacktrace);
    }
  }

  private String prepareExceptionMessage(TaskSubmissionException exception) {
    String message = exception.getMessage();
    Throwable cause = exception.getCause();
    if (cause != null) {
      message += String.format(" (Source issue: %s)", cause.getMessage());
    }
    return message;
  }
}

