package eu.europeana.cloud.service.dps.services.submitters;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.dps.EngineTaskState;
import eu.europeana.cloud.mcs.driver.RepresentationIterator;
import eu.europeana.cloud.service.dps.BatchInfo;
import eu.europeana.cloud.service.dps.DpsRecord;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.storm.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class MCSTaskSubmitter {

  public static final int LOGGING_FREQUENCY = 1000;

  private static final Logger LOGGER = LoggerFactory.getLogger(MCSTaskSubmitter.class);

  private final TaskStatusChecker taskStatusChecker;

  private final TaskStatusUpdater taskStatusUpdater;

  private final RecordSubmitService recordSubmitService;

  private final String mcsClientURL;
  private final String userName;
  private final String password;

  public MCSTaskSubmitter(TaskStatusChecker taskStatusChecker, TaskStatusUpdater taskStatusUpdater,
      RecordSubmitService recordSubmitService, String mcsClientURL, String userName, String password) {
    this.taskStatusChecker = taskStatusChecker;
    this.taskStatusUpdater = taskStatusUpdater;
    this.recordSubmitService = recordSubmitService;
    this.mcsClientURL = mcsClientURL;
    this.userName = userName;
    this.password = password;
  }

  public void execute(SubmitTaskParameters submitParameters) throws InterruptedException {
    DpsTask task = submitParameters.getTask();
    try {
      LOGGER.info("Sending task id={} to topology {} by kafka topic {}. Parameters:\n{}",
          task.getTaskId(), submitParameters.getTaskInfo().getTopologyName(), submitParameters.getTopicName(), submitParameters);

      checkIfTaskIsKilled(task);

      logProgress(submitParameters, 0);
      ExpectedCounters expectedCounters = executeForMCSInput(submitParameters);

      checkIfTaskIsKilled(task);
      if (!expectedCounters.areEmpty()) {
        taskStatusUpdater.updateStatusAndExpected(task.getTaskId(), EngineTaskState.QUEUED, expectedCounters);
        LOGGER.info("Submitting {} records of task id={} to Kafka succeeded.", expectedCounters.getExpectedRecords(), task.getTaskId());
      } else {
        taskStatusUpdater.setTaskDropped(task.getTaskId(), "The task was dropped because it is empty");
        LOGGER.warn("The task id={} was dropped because it is empty.", task.getTaskId());
      }

    } catch (TaskDroppedException e) {
      LOGGER.warn("Task was dropped while it was submitting to the topology! Task id: {}", e.getTaskId(), e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new InterruptedException(
          String.format("MCS service encountered interruption exception for taskId=%s with message: %s and stack trace: %s",
              task.getTaskId(), e.getMessage(), Arrays.toString(e.getStackTrace())));
    } catch (Exception e) {
      LOGGER.error("MCSTaskSubmitter error for taskId={}", task.getTaskId(), e);
      taskStatusUpdater.setTaskDropped(task.getTaskId(), "The task was dropped because " + e.getMessage());
    }
  }

  private MCSReader createMcsReader() {
    return new MCSReader(mcsClientURL, userName, password);
  }

  private ExpectedCounters executeForMCSInput(SubmitTaskParameters submitParameters)
      throws InterruptedException {
    try (var reader = createMcsReader()) {
      var expectedSize = ExpectedCounters.expectZeroRecords();
      BatchInfo input = (BatchInfo) submitParameters.getTask().getInput();
      expectedSize.add(executeForEntireDataset(input, submitParameters, reader));
      return expectedSize;

    }
  }

  private ExpectedCounters executeForEntireDataset(BatchInfo input, SubmitTaskParameters submitParameters, MCSReader reader) {
    var expectedSize = ExpectedCounters.expectZeroRecords();
    RepresentationIterator iterator = reader.getRepresentationsOfEntireDataset(input);
    while (iterator.hasNext()) {
      checkIfTaskIsKilled(submitParameters.getTask());
      Representation representation = iterator.next();
      expectedSize.add(submitRecordsForRepresentation(representation, submitParameters));
    }
    return expectedSize;
  }

  private ExpectedCounters submitRecordsForRepresentation(Representation representation, SubmitTaskParameters submitParameters) {
    if (representation == null) {
      throw new TaskSubmitException("Problem while reading representation - representation is null.");
    }
    if (representation.isMarkDepublished()) {
      return submitRecordForDeletedRepresentation(representation.getUri(), submitParameters);
    } else {
      return submitRecordsForFileObjects(representation.getFiles(), submitParameters);
    }
  }

  private ExpectedCounters submitRecordForDeletedRepresentation(URI representationVersionUri, SubmitTaskParameters submitParameters) {
    checkIfTaskIsKilled(submitParameters.getTask());

    if (submitRecord(representationVersionUri.toString(), submitParameters, true)) {
      return ExpectedCounters.expectSingleDepublishRecord();
    } else {
      return ExpectedCounters.expectZeroRecords();
    }
  }

  private ExpectedCounters submitRecordsForFileObjects(List<File> files, SubmitTaskParameters submitParameters) {
    return submitRecordsForFileUrlsList(files.stream().map(file -> file.getContentUri().toString()).collect(Collectors.toList()), submitParameters);
  }

  private ExpectedCounters submitRecordsForFileUrlsList(List<String> fileUrls, SubmitTaskParameters submitParameters) {
    ExpectedCounters baseCounter = ExpectedCounters.expectZeroRecords();

    for (String fileUrl : fileUrls) {
      checkIfTaskIsKilled(submitParameters.getTask());
      if (submitRecord(fileUrl, submitParameters, false)) {
        baseCounter.add(ExpectedCounters.expectSingleRecord());
      }

    }

    return baseCounter;
  }

  private boolean submitRecord(String fileUrl, SubmitTaskParameters submitParameters, boolean markedAsDepublished) {
    DpsTask task = submitParameters.getTask();
    DpsRecord aRecord = DpsRecord.builder()
                                 .taskId(task.getTaskId())
                                 .metadataPrefix(submitParameters.getSchemaName())
                                 .recordId(fileUrl)
            .markedAsDepublished(markedAsDepublished)
                                 .build();

    boolean increaseCounter = recordSubmitService.submitRecord(aRecord, submitParameters);
    logProgress(submitParameters, submitParameters.incrementAndGetPerformedRecordCounter());
    return increaseCounter;
  }

  private void logProgress(SubmitTaskParameters submitParameters, int submittedCount) {
    if (submittedCount % LOGGING_FREQUENCY == 0) {
      LOGGER.info("Task id={} records submitting is progressing. Already submitted: {} records",
          submitParameters.getTask().getTaskId(), submittedCount);
    }
  }

  private void checkIfTaskIsKilled(DpsTask task) {
    taskStatusChecker.checkNotDropped(task);
  }
}
