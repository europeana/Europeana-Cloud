package eu.europeana.cloud.service.dps.services.postprocessors;

import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.commons.utils.DateHelper;
import eu.europeana.cloud.service.commons.utils.RetryInterruptedException;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.metis.indexing.DataSetCleanerParameters;
import eu.europeana.cloud.service.dps.metis.indexing.DatasetCleaner;
import eu.europeana.cloud.service.dps.metis.indexing.DatasetCleaningException;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.service.utils.indexing.IndexWrapper;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsBatchCleaner;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
import java.util.Set;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexingPostProcessor extends TaskPostProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(IndexingPostProcessor.class);
  private static final int DELETE_ATTEMPTS = 20;
  private static final int DELAY_BETWEEN_DELETE_ATTEMPTS_MS = 30000;
  private static final Set<String> PROCESSED_TOPOLOGIES = Set.of(TopologiesNames.INDEXING_TOPOLOGY);
  private final IndexWrapper indexWrapper;

  public IndexingPostProcessor(TaskStatusUpdater taskStatusUpdater, HarvestedRecordsDAO harvestedRecordsDAO,
      TaskStatusChecker taskStatusChecker, IndexWrapper indexWrapper) {
    super(taskStatusChecker, taskStatusUpdater, harvestedRecordsDAO);
    this.indexWrapper = indexWrapper;
  }

  @Override
  public void executePostprocessing(TaskInfo taskInfo, DpsTask dpsTask) {
    try {
      taskStatusUpdater.updateState(dpsTask.getTaskId(), TaskState.IN_POST_PROCESSING,
          TaskState.IN_POST_PROCESSING.getDefaultMessage());
      LOGGER.info("Started postprocessing for {}", dpsTask);
      DataSetCleanerParameters cleanerParameters = prepareParameters(dpsTask);
      LOGGER.info("Parameters that will be used in postprocessing: {}", cleanerParameters);
      if (!areParametersNull(cleanerParameters)) {
        var datasetCleaner = new DatasetCleaner(indexWrapper, cleanerParameters);
        taskStatusUpdater.updateExpectedPostProcessedRecordsNumber(dpsTask.getTaskId(), datasetCleaner.getRecordsCount());
        Stream<String> recordIdsThatWillBeRemoved = datasetCleaner.getRecordIds();
        int deletedCount = cleanInECloud(cleanerParameters, recordIdsThatWillBeRemoved, dpsTask);
        cleanInMetis(cleanerParameters, datasetCleaner);
        taskStatusUpdater.updatePostProcessedRecordsCount(dpsTask.getTaskId(), deletedCount);
        endTheTask(dpsTask);
      } else {
        taskStatusUpdater.setTaskDropped(dpsTask.getTaskId(), "cleaner parameters can not be null");
      }
    } catch (RetryInterruptedException e) {
      throw e;
    } catch (Exception exception) {
      throw new PostProcessingException(
          String.format("Error while %s post-process given task: taskId=%d. Dataset was not removed correctly. Cause: %s",
              getClass().getSimpleName(), dpsTask.getTaskId(),
              exception.getMessage() != null ? exception.getMessage() : exception.toString()), exception);
    }
  }

  public Set<String> getProcessedTopologies() {
    return PROCESSED_TOPOLOGIES;
  }

  private DataSetCleanerParameters prepareParameters(DpsTask dpsTask) {
    return new DataSetCleanerParameters(
        dpsTask.getParameter(PluginParameterKeys.METIS_DATASET_ID),
        dpsTask.getParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE),
        DateHelper.parseISODate(dpsTask.getParameter(PluginParameterKeys.METIS_RECORD_DATE)));
  }

  private int cleanInECloud(DataSetCleanerParameters cleanerParameters, Stream<String> recordIds, DpsTask dpsTask) {
    TargetIndexingDatabase indexingDatabase;
    try {
      indexingDatabase = TargetIndexingDatabase.valueOf(cleanerParameters.getTargetIndexingEnv());
    } catch (IllegalArgumentException | NullPointerException exception) {
      throw new PostProcessingException("Unable to recognize environment: " + cleanerParameters.getTargetIndexingEnv());
    }
    return cleanDateAndMd5(recordIds, cleanerParameters.getDataSetId(), indexingDatabase, dpsTask);
  }

  private int cleanDateAndMd5(Stream<String> recordIds, String metisDatasetId, TargetIndexingDatabase indexingDatabase,
      DpsTask dpsTask) {
    HarvestedRecordsBatchCleaner cleaner = new HarvestedRecordsBatchCleaner(harvestedRecordsDAO, metisDatasetId,
        indexingDatabase);
    try (cleaner) {
      recordIds.takeWhile(harvestedRecord -> !taskIsDropped(dpsTask)).forEach(cleaner::executeRecord);
    }
    LOGGER.info("Cleaned indexing columns in Harvested records table for {} records.", cleaner.getCleanedCount());
    return cleaner.getCleanedCount();
  }

  private void cleanInMetis(DataSetCleanerParameters cleanerParameters, DatasetCleaner datasetCleaner)
      throws DatasetCleaningException {
    LOGGER.info("cleaning dataset {} based on date: {}",
        cleanerParameters.getDataSetId(), cleanerParameters.getCleaningDate());
    //The retry number and delay are relatively big cause, do not execute this task would cause inconsistency
    //between harvested_records table and state of Metis.
    RetryableMethodExecutor.execute("Could not clean old records in Metis", DELETE_ATTEMPTS,
        DELAY_BETWEEN_DELETE_ATTEMPTS_MS, () -> {
          datasetCleaner.execute();
          return null;
        });
    LOGGER.info("Dataset {} cleaned successfully", cleanerParameters.getDataSetId());
  }


  private void endTheTask(DpsTask dpsTask) {
    taskStatusUpdater.setTaskCompletelyProcessed(dpsTask.getTaskId(), "Completely process");
  }

  boolean areParametersNull(DataSetCleanerParameters cleanerParameters) {
    return cleanerParameters == null ||
        (cleanerParameters.getDataSetId() == null
            && cleanerParameters.getTargetIndexingEnv() == null
            && cleanerParameters.getCleaningDate() == null);
  }

  public boolean needsPostProcessing(DpsTask task) {
    return !isIncrementalIndexing(task);
  }

  private boolean isIncrementalIndexing(DpsTask task) {
    return "true".equals(task.getParameter(PluginParameterKeys.INCREMENTAL_INDEXING));
  }
}
