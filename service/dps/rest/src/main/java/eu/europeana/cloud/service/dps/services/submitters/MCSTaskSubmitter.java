package eu.europeana.cloud.service.dps.services.submitters;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
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

public class MCSTaskSubmitter implements TaskSubmitter {

  public static final int LOGGING_FREQUENCY = 1000;

  private static final Logger LOGGER = LoggerFactory.getLogger(MCSTaskSubmitter.class);

  private final TaskStatusChecker taskStatusChecker;

  private final RecordSubmitService recordSubmitService;

  private final String mcsClientURL;
  private final String userName;
  private final String password;

  public MCSTaskSubmitter(TaskStatusChecker taskStatusChecker,
      RecordSubmitService recordSubmitService, String mcsClientURL, String userName, String password) {
    this.taskStatusChecker = taskStatusChecker;
    this.recordSubmitService = recordSubmitService;
    this.mcsClientURL = mcsClientURL;
    this.userName = userName;
    this.password = password;
  }

  public ExpectedCounters submitTask(SubmitTaskParameters submitParameters) throws InterruptedException {
    DpsTask task = submitParameters.getTask();
    try {
      LOGGER.info("Sending task id={} to topology {} by kafka topic {}. Parameters:\n{}",
          task.getTaskId(), submitParameters.getTaskInfo().getTopologyName(), submitParameters.getTopicName(), submitParameters);

      checkIfTaskIsKilled(task);

      logProgress(submitParameters, 0);
      return executeForMCSInput(submitParameters);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new InterruptedException(
          String.format("MCS service encountered interruption exception for taskId=%s with message: %s and stack trace: %s",
              task.getTaskId(), e.getMessage(), Arrays.toString(e.getStackTrace())));
    } catch (Exception e) {
      throw new TaskSubmitException("MCSTaskSubmitter error for taskId: "+ task.getTaskId()+" : "+e.getMessage(), e);
    }
  }

  private MCSReader createMcsReader() {
    return new MCSReader(mcsClientURL, userName, password);
  }

  private ExpectedCounters executeForMCSInput(SubmitTaskParameters submitParameters)
      throws InterruptedException, TaskDroppedException {
    try (var reader = createMcsReader()) {
      var expectedSize = ExpectedCounters.expectZeroRecords();
      BatchInfo input = (BatchInfo) submitParameters.getTask().getSource();
      expectedSize.add(executeForEntireDataset(input, submitParameters, reader));
      return expectedSize;

    }
  }

  private ExpectedCounters executeForEntireDataset(BatchInfo input, SubmitTaskParameters submitParameters, MCSReader reader)
      throws TaskDroppedException {
    var expectedSize = ExpectedCounters.expectZeroRecords();
    RepresentationIterator iterator = reader.getRepresentationsOfEntireDataset(input);
    while (iterator.hasNext()) {
      checkIfTaskIsKilled(submitParameters.getTask());
      Representation representation = iterator.next();
      expectedSize.add(submitRecordsForRepresentation(representation, submitParameters));
    }
    return expectedSize;
  }

  private ExpectedCounters submitRecordsForRepresentation(Representation representation, SubmitTaskParameters submitParameters)
      throws TaskDroppedException {
    if (representation == null) {
      throw new TaskSubmitException("Problem while reading representation - representation is null.");
    }
    if (representation.isMarkDepublished()) {
      return submitRecordForDeletedRepresentation(representation.getUri(), submitParameters);
    } else {
      return submitExistingRepresentation(representation, submitParameters);
    }
  }

  protected ExpectedCounters submitExistingRepresentation(Representation representation, SubmitTaskParameters submitParameters)
      throws TaskDroppedException {
    return submitRecordsForAllFilesOfRepresentation(representation.getFiles(), submitParameters);
  }

  private ExpectedCounters submitRecordForDeletedRepresentation(URI representationVersionUri, SubmitTaskParameters submitParameters)
      throws TaskDroppedException {
    checkIfTaskIsKilled(submitParameters.getTask());

    if (submitRecord(representationVersionUri.toString(), submitParameters, true)) {
      return ExpectedCounters.expectSingleDepublishRecord();
    } else {
      return ExpectedCounters.expectZeroRecords();
    }
  }

  private ExpectedCounters submitRecordsForAllFilesOfRepresentation(List<File> files, SubmitTaskParameters submitParameters)
      throws TaskDroppedException {
    ExpectedCounters baseCounter = ExpectedCounters.expectZeroRecords();

    for (File file : files) {
      checkIfTaskIsKilled(submitParameters.getTask());
      String fileUrl = file.getContentUri().toString();
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

  private void checkIfTaskIsKilled(DpsTask task) throws TaskDroppedException {
    taskStatusChecker.checkNotDropped(task);
  }
}
