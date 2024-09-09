package eu.europeana.cloud.service.dps.depublish;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.services.submitters.SubmitingTaskWasKilled;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import eu.europeana.cloud.service.dps.storm.utils.RecordStatusUpdater;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
import eu.europeana.indexing.exception.IndexingException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class DepublicationService {

  private static final Logger LOGGER = LoggerFactory.getLogger(DepublicationService.class);

  private static final long DEFAULT_PROGRESS_POLLING_PERIOD = 5_000;

  private long progressPollingPeriod = DEFAULT_PROGRESS_POLLING_PERIOD;

  private final TaskStatusChecker taskStatusChecker;
  private final DatasetDepublisher depublisher;
  private final TaskStatusUpdater taskStatusUpdater;
  private final RecordStatusUpdater recordStatusUpdater;
  private final HarvestedRecordsDAO harvestedRecordsDAO;


  public DepublicationService(TaskStatusChecker taskStatusChecker, DatasetDepublisher depublisher,
      TaskStatusUpdater taskStatusUpdater, RecordStatusUpdater recordStatusUpdater,
      HarvestedRecordsDAO harvestedRecordsDAO) {
    this.taskStatusChecker = taskStatusChecker;
    this.depublisher = depublisher;
    this.taskStatusUpdater = taskStatusUpdater;
    this.recordStatusUpdater = recordStatusUpdater;
    this.harvestedRecordsDAO = harvestedRecordsDAO;
  }

  private void waitForFinish(Future<Integer> future, SubmitTaskParameters parameters)
      throws ExecutionException, InterruptedException, IndexingException {

    waitForAllRecordsRemoved(future, parameters);
    taskStatusUpdater.setTaskCompletelyProcessed(parameters.getTask().getTaskId(), "Dataset was depublished.");
    LOGGER.info("Task {} succeed ", parameters);
  }

  public void depublishDataset(SubmitTaskParameters parameters) {
    try {
      long taskId = parameters.getTask().getTaskId();
      checkTaskKilled(taskId);
      Future<Integer> future = depublisher.executeDatasetDepublicationAsync(parameters);
      waitForFinish(future, parameters);
      cleanAllDatasetRecordsInHarvestedRecordsTable(parameters);
    } catch (SubmitingTaskWasKilled e) {
      LOGGER.warn(e.getMessage(), e);
    } catch (InterruptedException e) {
      //We do not set failed task status in Cassandra in hope that task would be continued
      // by UnfinishedTaskExecutor, while applicaction server would start again
      LOGGER.warn("Depublication was interrupted!", e);
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      //Any other exception is caught, cause the method is executed asynchronously and the exception would
      //be eventually only logged. Instead of that the task result is stored in Cassandra, and by this way it
      //would be accessible to the user.
      saveErrorResult(parameters, e);
    }
  }

  public void depublishIndividualRecords(SubmitTaskParameters parameters) {
    LOGGER.info("Removing individual records from index");
    String[] records = parameters.getTaskParameter(PluginParameterKeys.RECORD_IDS_TO_DEPUBLISH).split(",");
    int errors = 0;
    for (int i = 0; i < records.length; i++) {
      int resourceNum = i + 1;
      try {
        LOGGER.info("Removing record with id '{}' from index", records[i]);
        checkTaskKilled(parameters.getTask().getTaskId());
        boolean removedSuccessfully = depublisher.removeRecord(records[i]);

        if (removedSuccessfully) {
          cleanRecordInHarvestedRecordsTable(parameters, records[i]);
          recordStatusUpdater.addSuccessfullyProcessedRecord(resourceNum, parameters.getTask().getTaskId(),
              TopologiesNames.DEPUBLICATION_TOPOLOGY, records[i]);
        } else {
          recordStatusUpdater.addWronglyProcessedRecord(resourceNum, parameters.getTask().getTaskId(),
              TopologiesNames.DEPUBLICATION_TOPOLOGY, records[i], null, null);
          errors++;
        }

      } catch (SubmitingTaskWasKilled e) {
        LOGGER.warn(e.getMessage(), e);
        return;
      } catch (Exception e) {
        //Any other exception is caught to perform independently as many records as it could be possible.
        //Anyway the method is executed asynchronously and the exception would be eventually only logged.
        //Instead of that the results are stored in Cassandra, and by this way they are accessible to the user.
        LOGGER.warn("Depublishing record {} ended with exception {}", records[i], e);
        recordStatusUpdater.addWronglyProcessedRecord(
            resourceNum,
            parameters.getTask().getTaskId(),
            TopologiesNames.DEPUBLICATION_TOPOLOGY,
            records[i], e.getMessage(), ExceptionUtils.getStackTrace(e));
        errors++;
      }
      saveProgress(parameters.getTask().getTaskId(), resourceNum, errors);
    }
    taskStatusUpdater.setTaskCompletelyProcessed(parameters.getTask().getTaskId(), "Dataset was depublished.");
    LOGGER.info("Records removal procedure finished for task_id {}", parameters.getTask().getTaskId());
  }

  private void waitForAllRecordsRemoved(Future<Integer> future, SubmitTaskParameters parameters)
      throws InterruptedException, IndexingException, ExecutionException {

    while (true) {
      long recordsLeft = depublisher.getRecordsCount(parameters);
      saveProgress(parameters.getTask().getTaskId(), parameters.getTaskInfo().getExpectedRecordsNumber() - recordsLeft);
      checkRemoveInvocationFinished(future, parameters.getTaskInfo().getExpectedRecordsNumber());
      if (recordsLeft == 0) {
        return;
      }
      Thread.sleep(progressPollingPeriod);
    }

  }

  protected void setProgressPollingPeriod(long timeInMs) {
    this.progressPollingPeriod = timeInMs;
  }

  private void checkRemoveInvocationFinished(Future<Integer> future, long expectedSize)
      throws InterruptedException, ExecutionException {
    if (future.isDone()) {
      int removedCount = future.get();
      if (removedCount != expectedSize) {
        throw new DepublicationException("Removed " + removedCount + "  rows! But expected to remove " + expectedSize);
      }
    }
  }

  private void checkTaskKilled(long taskId) {
    if (taskStatusChecker.hasDroppedStatus(taskId)) {
      throw new SubmitingTaskWasKilled(taskId);
    }
  }

  private void saveProgress(long taskId, long processed) {
    saveProgress(taskId, (int) processed, 0);
  }

  private void saveProgress(long taskId, int processed, int errors) {
    taskStatusUpdater.setUpdateProcessedFiles(taskId, processed, 0, 0, errors, 0);
  }

  private void saveErrorResult(SubmitTaskParameters parameters, Exception e) {
    String fullStacktrace = ExceptionUtils.getStackTrace(e);
    LOGGER.error("Task execution failed: {}", fullStacktrace);
    taskStatusUpdater.setTaskDropped(parameters.getTask().getTaskId(), fullStacktrace);
  }

  private void cleanAllDatasetRecordsInHarvestedRecordsTable(SubmitTaskParameters parameters) {
    String metisDatasetId = parameters.getTaskParameter(PluginParameterKeys.METIS_DATASET_ID);
    harvestedRecordsDAO.findDatasetRecords(metisDatasetId).forEachRemaining(this::cleanRecordInHarvestedRecordsTable);
  }

  private void cleanRecordInHarvestedRecordsTable(SubmitTaskParameters parameters, String recordId) {
    String metisDatasetId = parameters.getTaskParameter(PluginParameterKeys.METIS_DATASET_ID);
    harvestedRecordsDAO.findRecord(metisDatasetId, recordId).ifPresent(this::cleanRecordInHarvestedRecordsTable);
  }

  private void cleanRecordInHarvestedRecordsTable(HarvestedRecord theRecord) {
    theRecord.setPublishedHarvestDate(null);
    theRecord.setPublishedHarvestMd5(null);
    harvestedRecordsDAO.insertHarvestedRecord(theRecord);
  }

}
