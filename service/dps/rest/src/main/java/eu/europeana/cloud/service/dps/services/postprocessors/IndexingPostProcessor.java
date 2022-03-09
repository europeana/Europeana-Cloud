package eu.europeana.cloud.service.dps.services.postprocessors;

import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.metis.indexing.DataSetCleanerParameters;
import eu.europeana.cloud.service.dps.metis.indexing.DatasetCleaner;
import eu.europeana.cloud.service.dps.metis.indexing.DatasetCleaningException;
import eu.europeana.cloud.service.dps.service.utils.indexing.IndexWrapper;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase.*;

public class IndexingPostProcessor extends TaskPostProcessor {

    public static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
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

    private DataSetCleanerParameters prepareParameters(DpsTask dpsTask) throws ParseException {
        DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT, Locale.US);
        return new DataSetCleanerParameters(
                dpsTask.getParameter(PluginParameterKeys.METIS_DATASET_ID),
                dpsTask.getParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE),
                dateFormat.parse(dpsTask.getParameter(PluginParameterKeys.METIS_RECORD_DATE)));
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

    private int cleanDateAndMd5(Stream<String> recordIds, String datasetId, TargetIndexingDatabase indexingDatabase, DpsTask dpsTask) {
        var deletedCount = new AtomicInteger();
        recordIds
                .map(recordId -> harvestedRecordsDAO.findRecord(datasetId, recordId))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(harvestedRecord -> doCleaning(harvestedRecord, indexingDatabase))
                .takeWhile(harvestedRecord -> !taskIsDropped(dpsTask))
                .forEach(theRecord -> {
                    harvestedRecordsDAO.insertHarvestedRecord(theRecord);
                    deletedCount.incrementAndGet();
                });
        LOGGER.info("Cleaned indexing columns in Harvested records table for {} records.", deletedCount.get());
        return deletedCount.get();
    }

    private HarvestedRecord doCleaning(HarvestedRecord harvestedRecord, TargetIndexingDatabase indexingDatabase) {
        if (indexingDatabase == PREVIEW) {
            harvestedRecord.setPreviewHarvestDate(null);
            harvestedRecord.setPreviewHarvestMd5(null);
        } else if (indexingDatabase == PUBLISH) {
            harvestedRecord.setPublishedHarvestDate(null);
            harvestedRecord.setPublishedHarvestMd5(null);
        }
        return harvestedRecord;
    }

    private void cleanInMetis(DataSetCleanerParameters cleanerParameters, DatasetCleaner datasetCleaner) throws DatasetCleaningException {
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
}
