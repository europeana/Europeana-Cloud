package eu.europeana.cloud.service.dps.services.submitters;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.dps.EngineTaskState;
import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.common.response.RepresentationRevisionResponse;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.mcs.driver.RepresentationIterator;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.DpsRecord;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.InputDataType;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.TaskDroppedException;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static eu.europeana.cloud.service.dps.InputDataType.FILE_URLS;

public class MCSTaskSubmitter {

  public static final int LOGGING_FREQUENCY = 1000;
  private static final int INTERNAL_THREADS_NUMBER = 10;
  private static final int MAX_BATCH_SIZE = 100;

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
      ExpectedCounters expectedCounters;
      if (taskContainsFileUrls(task)) {
        expectedCounters = executeForFilesList(submitParameters);
      } else {
        expectedCounters = executeForDatasetList(submitParameters);
      }

      checkIfTaskIsKilled(task);
      if (!expectedCounters.areEmpty()) {
        taskStatusUpdater.updateStatusExpectedRecordsAndExpectedDepublishRecords(task.getTaskId(), EngineTaskState.QUEUED, expectedCounters.expectedRecords, expectedCounters.expectedDepublishRecords);
        LOGGER.info("Submitting {} records of task id={} to Kafka succeeded.", expectedCounters.expectedRecords, task.getTaskId());
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

  private ExpectedCounters executeForFilesList(SubmitTaskParameters submitParameters) {
    List<String> fileUrlsList = submitParameters.getTask().getDataEntry(FILE_URLS);
    return submitRecordsForFileUrlsList(fileUrlsList, submitParameters);
  }

  private ExpectedCounters executeForDatasetList(SubmitTaskParameters submitParameters)
      throws InterruptedException, MCSException, ExecutionException {
    var counter = ExpectedCounters.expectZeroRecords();
    for (String dataSetUrl : submitParameters.getTask().getDataEntry(InputDataType.DATASET_URLS)) {
      counter.add(executeForOneDataSet(dataSetUrl, submitParameters));
    }
    return counter;
  }

  private ExpectedCounters executeForOneDataSet(String dataSetUrl, SubmitTaskParameters submitParameters)
      throws InterruptedException, MCSException, ExecutionException {
    try (var reader = createMcsReader()) {
      ExpectedCounters counter;
      var urlParser = new UrlParser(dataSetUrl);
      if (!urlParser.isUrlToDataset()) {
        throw new TaskSubmitException("DataSet URL is not formulated correctly: " + dataSetUrl);
      }

      if (submitParameters.hasInputRevision()) {
        counter = executeForRevision(urlParser.getPart(UrlPart.DATA_SETS), urlParser.getPart(UrlPart.DATA_PROVIDERS),
                submitParameters, reader);
      } else {
        counter = executeForEntireDataset(urlParser, submitParameters, reader);

      }
      return counter;

    } catch (MalformedURLException e) {
      throw new TaskSubmitException("MCSTaskSubmitter error, Error while parsing DataSet URL : \"" + dataSetUrl + "\"", e);
    }
  }

  private ExpectedCounters executeForEntireDataset(UrlParser urlParser, SubmitTaskParameters submitParameters, MCSReader reader) {
    var counter = ExpectedCounters.expectZeroRecords();
    RepresentationIterator iterator = reader.getRepresentationsOfEntireDataset(urlParser);
    while (iterator.hasNext()) {
      checkIfTaskIsKilled(submitParameters.getTask());
      Representation representation = iterator.next();
      counter.add(submitRecordsForRepresentation(representation, submitParameters));
    }
    return counter;
  }

  private ExpectedCounters executeForRevision(String datasetName, String datasetProvider, SubmitTaskParameters submitParameters,
                                              MCSReader reader) throws InterruptedException, MCSException, ExecutionException {
    ExecutorService executor = Executors.newFixedThreadPool(INTERNAL_THREADS_NUMBER);
    try {
      DpsTask task = submitParameters.getTask();
      var maxRecordsCount = submitParameters.getMaxRecordsCount();
      var counter = ExpectedCounters.expectZeroRecords();
      String startFrom = null;
      Set<Future<ExpectedCounters>> futures = HashSet.newHashSet(INTERNAL_THREADS_NUMBER);
      var total = 0;
      do {
        checkIfTaskIsKilled(task);
        ResultSlice<CloudTagsResponse> slice = getCloudIdsChunk(datasetName, datasetProvider, startFrom, submitParameters,
            reader);
        List<CloudTagsResponse> cloudTagsResponseList = slice.getResults();

        int maxRecordsLeft = maxRecordsCount - total;
        if (cloudTagsResponseList.size() > maxRecordsLeft) {
          cloudTagsResponseList = cloudTagsResponseList.subList(0, maxRecordsLeft);
        }
        total += cloudTagsResponseList.size();

        final List<CloudTagsResponse> finalCloudTagsResponseList = cloudTagsResponseList;
        futures.add(
            executor.submit(() -> executeGettingFileUrlsForCloudIdList(finalCloudTagsResponseList, submitParameters, reader)));

        if (futures.size() >= INTERNAL_THREADS_NUMBER * MAX_BATCH_SIZE) {
          counter.add(getCountAndWait(futures));
        }
        startFrom = slice.getNextSlice();
      }
      while ((startFrom != null) && (total < maxRecordsCount));

      if (!futures.isEmpty()) {
        counter.add(getCountAndWait(futures));
      }

      return counter;
    } catch (ExecutionException e) {
      if (e.getCause() instanceof TaskDroppedException) {
        LOGGER.debug("Caught ExecutionException from Threads executor. Task was killed.");
        throw new TaskDroppedException(submitParameters.getTask());
      } else {
        throw e;
      }
    } finally {
      executor.shutdown();
      executor.awaitTermination(1, TimeUnit.MINUTES);
    }
  }

  private ResultSlice<CloudTagsResponse> getCloudIdsChunk(String datasetName, String datasetProvider,
      String startFrom, SubmitTaskParameters submitTaskParameters,
      MCSReader reader) throws MCSException {
    return reader.getDataSetRevisionsChunk(
        submitTaskParameters.getRepresentationName(),
        submitTaskParameters.getInputRevision(),
        datasetProvider, datasetName, startFrom);
  }

  private ExpectedCounters executeGettingFileUrlsForCloudIdList(List<CloudTagsResponse> responseList,
                                                                SubmitTaskParameters submitParameters,
                                                                MCSReader reader) throws MCSException {
    ExpectedCounters counter = ExpectedCounters.expectZeroRecords();
    for (CloudTagsResponse response : responseList) {
      counter.add(executeGettingFileUrlsForOneCloudId(response, submitParameters, reader));
    }

    return counter;
  }

  private ExpectedCounters executeGettingFileUrlsForOneCloudId(CloudTagsResponse response, SubmitTaskParameters submitParameters,
                                                               MCSReader reader) throws MCSException {

    ExpectedCounters counter = ExpectedCounters.expectZeroRecords();
    checkIfTaskIsKilled(submitParameters.getTask());
    List<RepresentationRevisionResponse> representationRevisions = reader.getRevisionsForTheRepresentation(
        submitParameters.getRepresentationName(),
        submitParameters.getInputRevision().getRevisionName(),
        submitParameters.getInputRevision().getRevisionProviderId(),
        submitParameters.getInputRevision().getCreationTimeStamp(),
        response.getCloudId());
    for (RepresentationRevisionResponse representationRevision : representationRevisions) {
      counter.add(submitRecordsForRepresentationRevision(representationRevision, submitParameters, response.isDeleted()));
    }
    return counter;
  }

  private ExpectedCounters submitRecordsForRepresentationRevision(RepresentationRevisionResponse representationRevision,
                                                                  SubmitTaskParameters submitParameters, boolean markedAsDepublished) {
    if (markedAsDepublished) {
      return submitRecordForDeletedRepresentation(representationRevision.getRepresentationVersionUri(), submitParameters);
    } else {
      return submitRecordsForFileObjects(representationRevision.getFiles(), submitParameters);
    }
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

  private boolean taskContainsFileUrls(DpsTask task) {
    return task.getInputData().get(FILE_URLS) != null;
  }

  private ExpectedCounters getCountAndWait(Set<Future<ExpectedCounters>> futures) throws InterruptedException, ExecutionException {
    ExpectedCounters counter = ExpectedCounters.expectZeroRecords();
    for (Future<ExpectedCounters> future : futures) {
      counter.add(future.get());
    }
    futures.clear();
    return counter;
  }

  private void checkIfTaskIsKilled(DpsTask task) {
    taskStatusChecker.checkNotDropped(task);
  }

  /**
   * Private helper class to track expected record counts.
   */
  @Getter
  private static class ExpectedCounters {
    private int expectedRecords;
    private int expectedDepublishRecords;

    public ExpectedCounters(int expectedRecords, int expectedDepublishRecords) {
      this.expectedRecords = expectedRecords;
      this.expectedDepublishRecords = expectedDepublishRecords;
    }

    public static ExpectedCounters expectSingleRecord() {
      return new ExpectedCounters(1, 0);
    }

    public static ExpectedCounters expectSingleDepublishRecord() {
      return new ExpectedCounters(0, 1);
    }

    public static ExpectedCounters expectZeroRecords() {
      return new ExpectedCounters(0, 0);
    }

    /**
     * Add another Counters instance values into this one values.
     */
    public void add(ExpectedCounters other) {
      if (other == null) {
        return;
      }
      this.expectedRecords += other.expectedRecords;
      this.expectedDepublishRecords += other.expectedDepublishRecords;
    }

    public boolean areEmpty() {
      return this.expectedRecords == 0 && this.expectedDepublishRecords == 0;
    }
  }
}
