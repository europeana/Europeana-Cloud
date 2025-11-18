package eu.europeana.cloud.service.dps.services.submitters;

import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.HarvestResult;
import eu.europeana.cloud.service.dps.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.dps.utils.DpsTaskToOaiHarvestConverter;
import eu.europeana.cloud.service.dps.utils.HarvestsExecutor;
import eu.europeana.cloud.service.dps.utils.KafkaTopicSelector;
import eu.europeana.cloud.service.dps.utils.files.counter.FilesCounterFactory;
import eu.europeana.metis.harvesting.HarvesterException;
import eu.europeana.metis.harvesting.HarvesterRuntimeException;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class OaiTopologyTaskSubmitter implements TaskSubmitter {

  private static final Logger LOGGER = LoggerFactory.getLogger(OaiTopologyTaskSubmitter.class);

  private final HarvestsExecutor harvestsExecutor;
  private final KafkaTopicSelector kafkaTopicSelector;
  private final FilesCounterFactory filesCounterFactory;
  private final TaskStatusUpdater taskStatusUpdater;

  public OaiTopologyTaskSubmitter(HarvestsExecutor harvestsExecutor,
      KafkaTopicSelector kafkaTopicSelector,
      FilesCounterFactory filesCounterFactory,
      TaskStatusUpdater taskStatusUpdater
  ) {
    this.harvestsExecutor = harvestsExecutor;
    this.kafkaTopicSelector = kafkaTopicSelector;
    this.filesCounterFactory = filesCounterFactory;
    this.taskStatusUpdater = taskStatusUpdater;
  }

  @Override
  public void submitTask(SubmitTaskParameters parameters) throws TaskSubmissionException {

    LOGGER.info("Starting OAI harvesting for: {}", parameters);

    int expectedCount = getFilesCountInsideTask(parameters);
    LOGGER.info("The task {} is in a pending mode.Expected size: {}", parameters.getTask().getTaskId(), expectedCount);

    if (expectedCount == 0) {
      taskStatusUpdater.setTaskDropped(parameters.getTask().getTaskId(), "The task doesn't include any records");
      return;
    }

    String preferredTopicName = kafkaTopicSelector.findPreferredTopicNameFor(parameters.getTaskInfo().getTopologyName());
    parameters.setTopicName(preferredTopicName);
    parameters.getTaskInfo().setStateDescription("Task submitted successfully and processed by REST app");
    parameters.getTaskInfo().setExpectedRecordsNumber(expectedCount);
    parameters.getTaskInfo().setState(TaskState.PROCESSING_BY_REST_APPLICATION);
    LOGGER.info("Selected topic name: {} for {}", preferredTopicName, parameters.getTask().getTaskId());
    taskStatusUpdater.updateSubmitParameters(parameters);

    OaiHarvest harvestToByExecuted = new DpsTaskToOaiHarvestConverter().from(parameters.getTask());
    try {
      HarvestResult harvesterResult;
      harvesterResult = harvestsExecutor.execute(harvestToByExecuted, parameters);
      updateTaskStatus(parameters.getTask().getTaskId(), harvesterResult);
    } catch (HarvesterRuntimeException e) {
      throw new TaskSubmissionException("Unable to submit task properly", e);
    }
  }

  private int getFilesCountInsideTask(SubmitTaskParameters parameters) throws TaskSubmissionException {
    return filesCounterFactory.createFilesCounter(parameters.getTask(), parameters.getTaskInfo().getTopologyName())
                              .getFilesCount(parameters.getTask());
  }

  private void updateTaskStatus(long taskId, HarvestResult harvesterResult) {
    if (harvesterResult.getTaskState() != TaskState.DROPPED && harvesterResult.getResultCounter() == 0) {
      LOGGER.info("Task dropped. No data harvested");
      taskStatusUpdater.setTaskDropped(taskId, "The task with the submitted parameters is empty");
    } else {
      LOGGER.info("Updating task {} expected size to: {}", taskId, harvesterResult.getResultCounter());
      taskStatusUpdater.updateStatusExpectedSize(taskId, harvesterResult.getTaskState(),
          harvesterResult.getResultCounter());
    }
  }
}
