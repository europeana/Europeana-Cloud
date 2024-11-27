package eu.europeana.cloud.service.dps.services.submitters;

import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.DpsRecord;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.InputDataType;
import eu.europeana.cloud.service.dps.http.FileURLCreator;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
import eu.europeana.cloud.service.dps.utils.KafkaTopicSelector;
import eu.europeana.metis.harvesting.HarvesterException;
import eu.europeana.metis.harvesting.HarvesterFactory;
import eu.europeana.metis.harvesting.HarvestingIterator;
import eu.europeana.metis.harvesting.ReportingIteration.IterationResult;
import java.io.File;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class HttpTopologyTaskSubmitter implements TaskSubmitter {

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpTopologyTaskSubmitter.class);

  private final TaskStatusUpdater taskStatusUpdater;
  private final KafkaTopicSelector kafkaTopicSelector;
  private final TaskStatusChecker taskStatusChecker;
  private final RecordSubmitService recordSubmitService;
  private final FileURLCreator fileURLCreator;

  @Value("${harvestingTasksDir}")
  private String harvestingTasksDir;

  public HttpTopologyTaskSubmitter(TaskStatusUpdater taskStatusUpdater,
      RecordSubmitService recordSubmitService,
      KafkaTopicSelector kafkaTopicSelector,
      TaskStatusChecker taskStatusChecker,
      FileURLCreator fileURLCreator) {
    this.taskStatusUpdater = taskStatusUpdater;
    this.recordSubmitService = recordSubmitService;
    this.kafkaTopicSelector = kafkaTopicSelector;
    this.taskStatusChecker = taskStatusChecker;
    this.fileURLCreator = fileURLCreator;
  }

  @Override
  public void submitTask(SubmitTaskParameters parameters) {

    LOGGER.info("HTTP task submission for {} started.", parameters.getTask().getTaskId());

    int expectedCount = -1;
    parameters.getTaskInfo().setExpectedRecordsNumber(expectedCount);
    LOGGER.info("The task {} is in a pending mode.Expected size: {}", parameters.getTask().getTaskId(), expectedCount);

    try {
      final String urlToZipFile = parameters.getTask()
                                            .getDataEntry(InputDataType.REPOSITORY_URLS).get(0);
      final HarvestingIterator<Path, Path> iterator = HarvesterFactory.createHttpHarvester()
                                                                      .harvestRecords(urlToZipFile,
                                                              downloadedFileLocationFor(parameters.getTask()));
      selectKafkaTopicFor(parameters);
      taskStatusUpdater.updateSubmitParameters(parameters);
      expectedCount = iterateOverFiles(iterator, parameters);
      updateTaskStatus(parameters.getTask(), expectedCount);
    } catch (HarvesterException e) {
      LOGGER.error("Unable to submit the task.", e);
      taskStatusUpdater.setTaskDropped(parameters.getTask().getTaskId(),
          "The task was dropped because of errors: " + e.getMessage());
    }

    LOGGER.info("HTTP task submission for {} finished. {} records submitted.",
        parameters.getTask().getTaskId(),
        expectedCount);
  }

  private String downloadedFileLocationFor(DpsTask dpsTask) {
    return harvestingTasksDir + File.separator + "task_" + dpsTask.getTaskId();
  }

  private void selectKafkaTopicFor(SubmitTaskParameters parameters) {
    parameters.setTopicName(kafkaTopicSelector.findPreferredTopicNameFor(TopologiesNames.HTTP_TOPOLOGY));
  }

  private int iterateOverFiles(HarvestingIterator<Path, Path> iterator,
      SubmitTaskParameters submitTaskParameters) throws HarvesterException {
    final var expectedSize = new AtomicInteger(0);
    iterator.forEach(file -> {
      if (taskStatusChecker.hasDroppedStatus(submitTaskParameters.getTask().getTaskId())) {
        return IterationResult.TERMINATE;
      }
      DpsRecord dpsRecord;
      try {
        dpsRecord = DpsRecord.builder()
                             .taskId(submitTaskParameters.getTask().getTaskId())
                             .recordId(fileURLCreator.generateUrlFor(file))
                             .build();
      } catch (UnsupportedEncodingException e) {
        taskStatusUpdater.setTaskDropped(submitTaskParameters.getTask().getTaskId(),
            "Unable to generate URL for file: " + file.toString());
        return IterationResult.TERMINATE;
      }

      if (recordSubmitService.submitRecord(
          dpsRecord,
          submitTaskParameters)) {
        expectedSize.incrementAndGet();
      }
      return IterationResult.CONTINUE;
    });
    return expectedSize.get();
  }

  private void updateTaskStatus(DpsTask dpsTask, int expectedCount) {
    if (!taskStatusChecker.hasDroppedStatus(dpsTask.getTaskId())) {
      if (expectedCount == 0) {
        taskStatusUpdater.setTaskDropped(dpsTask.getTaskId(), "The task doesn't include any records");
      } else {
        taskStatusUpdater.updateStatusExpectedSize(dpsTask.getTaskId(), TaskState.QUEUED, expectedCount);
      }
    }
  }
}
