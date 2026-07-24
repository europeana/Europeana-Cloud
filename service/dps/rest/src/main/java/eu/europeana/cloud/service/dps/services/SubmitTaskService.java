package eu.europeana.cloud.service.dps.services;

import eu.europeana.cloud.common.model.dps.EngineTaskState;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.service.dps.BatchInfo;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.services.submitters.TaskSubmitter;
import eu.europeana.cloud.service.dps.services.submitters.TaskSubmitterFactory;
import eu.europeana.cloud.service.dps.storm.dao.TaskDiagnosticInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.ExpectedCounters;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.TaskDroppedException;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.dps.utils.KafkaTopicSelector;
import eu.europeana.cloud.service.dps.utils.files.counter.FilesCounter;
import eu.europeana.cloud.service.dps.utils.files.counter.FilesCounterFactory;
import eu.europeana.cloud.service.mcs.exception.MCSException;
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
  private final TaskStatusChecker taskStatusChecker;
  private final TaskStatusUpdater taskStatusUpdater;
  private final TaskDiagnosticInfoDAO taskDiagnosticInfoDAO;
  private final DataSetServiceClient dataSetServiceClient;
  private final FilesCounterFactory filesCounterFactory;
  private final KafkaTopicSelector kafkaTopicSelector;

  public SubmitTaskService(TaskSubmitterFactory taskSubmitterFactory, TaskStatusUpdater taskStatusUpdater,
      TaskStatusChecker taskStatusChecker, TaskDiagnosticInfoDAO taskDiagnosticInfoDAO, DataSetServiceClient dataSetServiceClient,
      FilesCounterFactory filesCounterFactory, KafkaTopicSelector kafkaTopicSelector) {
    this.taskStatusChecker = taskStatusChecker;
    this.taskStatusUpdater = taskStatusUpdater;
    this.taskSubmitterFactory = taskSubmitterFactory;
    this.taskDiagnosticInfoDAO = taskDiagnosticInfoDAO;
    this.dataSetServiceClient = dataSetServiceClient;
    this.filesCounterFactory = filesCounterFactory;
    this.kafkaTopicSelector = kafkaTopicSelector;
  }

  @Async
  public void submitTask(SubmitTaskParameters parameters) {
    try {
      int estimatedRecordCount = estimateTaskSize(parameters);
      if (estimatedRecordCount == 0) {
        taskStatusUpdater.setTaskDropped(parameters.getTask().getTaskId(), "The task doesn't include any records");
        return;
      }

      createDateSetIfNeeded(parameters.getTask());
      selectKafkaTopic(parameters);
      taskStatusUpdater.updateSubmitParameters(parameters);
      submitPreparedTask(parameters);
    } catch (InterruptedException e) {
      LOGGER.warn("Task submission interrupted due to InterruptedException!", e);
      Thread.currentThread().interrupt();
    } catch (TaskDroppedException e) {
      LOGGER.warn("Task submission interrupted because task was dropped!", e);
      Thread.currentThread().interrupt();
    } catch (TaskSubmissionException e) {
      LOGGER.error("Task submission failed: {}", e.getMessage(), e);
      taskStatusUpdater.setTaskDropped(parameters.getTask().getTaskId(), prepareExceptionMessage(e));
    } catch (Exception e) {
      LOGGER.error("Task submission failed because of unexpected exception: {}", e.getMessage(),e);
      String fullStacktrace = ExceptionUtils.getStackTrace(e);
      taskStatusUpdater.setTaskDropped(parameters.getTask().getTaskId(), fullStacktrace);
    }
  }

  private int estimateTaskSize(SubmitTaskParameters parameters) throws TaskSubmissionException {
    int estimatedRecordCount = getFilesCountInsideTask(parameters.getTask(), parameters.getTaskInfo().getTopologyName());
    LOGGER.info("The task {} is in a pending mode.Expected size: {}", parameters.getTask().getTaskId(), estimatedRecordCount);
    parameters.getTaskInfo().setExpectedRecords(estimatedRecordCount);
    return estimatedRecordCount;
  }

  private void createDateSetIfNeeded(DpsTask dpsTask) throws TaskSubmissionException {
    try {
      BatchInfo results = dpsTask.getResultsBatch();
      if ((results != null) && !dataSetServiceClient.datasetExists(results.getProviderId(), results.getBatchId())) {
        dataSetServiceClient.createDataSet(results.getProviderId(), results.getBatchId(), createDescription(dpsTask));
      }

    } catch (MCSException e) {
      throw new TaskSubmissionException(
          "Couldn't connect to mcs to verify dataSet exists for task with id %s!".formatted(dpsTask.getTaskId()), e);
    }
  }

  private void selectKafkaTopic(SubmitTaskParameters parameters) {
    String preferredTopicName = kafkaTopicSelector.findPreferredTopicNameFor(parameters.getTaskInfo().getTopologyName());
    parameters.setTopicName(preferredTopicName);
  }

  private void submitPreparedTask(SubmitTaskParameters parameters)
      throws TaskSubmissionException, InterruptedException, TaskDroppedException {
    TaskSubmitter taskSubmitter = taskSubmitterFactory.provideTaskSubmitter(parameters);
    ExpectedCounters submittedCounter = taskSubmitter.submitTask(parameters);
    DpsTask task = parameters.getTask();
    taskStatusChecker.checkNotDropped(task);
    if (!submittedCounter.areEmpty()) {
      taskStatusUpdater.updateStatusAndExpected(task.getTaskId(), EngineTaskState.QUEUED, submittedCounter);
      LOGGER.info("Submitting {} records of task id={} to Kafka succeeded.", submittedCounter.getExpectedRecords(),
          task.getTaskId());
    } else {
      taskStatusUpdater.setTaskDropped(task.getTaskId(), "The task was dropped because it is empty");
      LOGGER.warn("The task id={} was dropped because it is empty.", task.getTaskId());
    }

    taskDiagnosticInfoDAO.updateQueuedTime(parameters.getTask().getTaskId(), Instant.now());
  }

  private String prepareExceptionMessage(TaskSubmissionException exception) {
    String message = exception.getMessage();
    Throwable cause = exception.getCause();
    if (cause != null) {
      message += String.format(" (Source issue: %s)", cause.getMessage());
    }
    return message;
  }


  private int getFilesCountInsideTask(DpsTask task, String topologyName) throws TaskSubmissionException {
    FilesCounter filesCounter = filesCounterFactory.createFilesCounter(task, topologyName);
    return filesCounter.getFilesCount(task);
  }

  private String createDescription(DpsTask dpsTask) {
    return "Output batch of the task named: " + dpsTask.getTaskName()
        + ", id: " + dpsTask.getTaskId()
        + ", run for Metis dataset: " + dpsTask.getParameter(PluginParameterKeys.METIS_DATASET_ID)
        + ", at: " + Instant.now();
  }
}

