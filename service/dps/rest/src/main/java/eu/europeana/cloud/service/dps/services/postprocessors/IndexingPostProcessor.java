package eu.europeana.cloud.service.dps.services.postprocessors;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.metis.indexing.DataSetCleanerParameters;
import eu.europeana.cloud.service.dps.metis.indexing.DatasetCleaner;
import eu.europeana.cloud.service.dps.service.utils.validation.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Stream;

public class IndexingPostProcessor implements TaskPostProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexingPostProcessor.class);

    private static final Set<String> PROCESSED_TOPOLOGIES = Set.of(TopologiesNames.INDEXING_TOPOLOGY);

    public static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";

    private final TaskStatusUpdater taskStatusUpdater;
    private final HarvestedRecordsDAO harvestedRecordsDAO;

    public IndexingPostProcessor(TaskStatusUpdater taskStatusUpdater, HarvestedRecordsDAO harvestedRecordsDAO) {
        this.taskStatusUpdater = taskStatusUpdater;
        this.harvestedRecordsDAO = harvestedRecordsDAO;
    }

    @Override
    public void execute(DpsTask dpsTask) {
        try {
            LOGGER.info("Started postprocessing for {}", dpsTask);
            DataSetCleanerParameters cleanerParameters = prepareParameters(dpsTask);
            LOGGER.info("Parameters that will be used in postprocessing: {}", cleanerParameters);
            if (!areParametersNull(cleanerParameters)) {
                var datasetCleaner = new DatasetCleaner(cleanerParameters);
                Stream<String> recordIdsThatWillBeRemoved = datasetCleaner.getRecordIds();
                LOGGER.info("cleaning dataset {} based on date: {}",
                        cleanerParameters.getDataSetId(), cleanerParameters.getCleaningDate());
                datasetCleaner.execute();
                LOGGER.info("Dataset {} cleaned successfully", cleanerParameters.getDataSetId());
                cleanECloud(recordIdsThatWillBeRemoved, cleanerParameters);
                endTheTask(dpsTask);
            } else {
                taskStatusUpdater.setTaskDropped(dpsTask.getTaskId(), "cleaner parameters can not be null");
            }
        } catch(Exception exception) {
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
                Boolean.parseBoolean(dpsTask.getParameter(PluginParameterKeys.METIS_USE_ALT_INDEXING_ENV)),
                dpsTask.getParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE),
                dateFormat.parse(dpsTask.getParameter(PluginParameterKeys.METIS_RECORD_DATE)));
    }

    private void cleanECloud(Stream<String> recordIds, DataSetCleanerParameters cleanerParameters) {
        if (isPreviewEnvironment(cleanerParameters)) {
            cleanPreviewDateAndMd5(recordIds, cleanerParameters.getDataSetId());
        } else if (isPublishEnvironment(cleanerParameters)) {
            cleanPublishDateAndMd5(recordIds, cleanerParameters.getDataSetId());
        } else {
            throw new PostProcessingException("Unable to recognize environment" + cleanerParameters.getTargetIndexingEnv());
        }
    }

    private void cleanPreviewDateAndMd5(Stream<String> recordIds, String datasetId) {
        recordIds
                .map(recordId -> harvestedRecordsDAO.findRecord(datasetId, recordId))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(harvestedRecord ->
                        HarvestedRecord.builder()
                                .metisDatasetId(harvestedRecord.getMetisDatasetId())
                                .recordLocalId(harvestedRecord.getRecordLocalId())
                                .latestHarvestDate(harvestedRecord.getLatestHarvestDate())
                                .latestHarvestMd5(harvestedRecord.getLatestHarvestMd5())
                                .previewHarvestDate(null)
                                .previewHarvestMd5(null)
                                .publishedHarvestDate(harvestedRecord.getPublishedHarvestDate())
                                .publishedHarvestMd5(harvestedRecord.getPublishedHarvestMd5())
                                .build())
                .forEach(harvestedRecordsDAO::insertHarvestedRecord);
    }

    private void cleanPublishDateAndMd5(Stream<String> recordIds, String datasetId) {
        recordIds
                .map(recordId -> harvestedRecordsDAO.findRecord(datasetId, recordId))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(harvestedRecord ->
                        HarvestedRecord.builder()
                                .metisDatasetId(harvestedRecord.getMetisDatasetId())
                                .recordLocalId(harvestedRecord.getRecordLocalId())
                                .latestHarvestDate(harvestedRecord.getLatestHarvestDate())
                                .latestHarvestMd5(harvestedRecord.getLatestHarvestMd5())
                                .previewHarvestDate(harvestedRecord.getPreviewHarvestDate())
                                .previewHarvestMd5(harvestedRecord.getPreviewHarvestMd5())
                                .publishedHarvestDate(null)
                                .publishedHarvestMd5(null)
                                .build())
                .forEach(harvestedRecordsDAO::insertHarvestedRecord);
    }

    private boolean isPublishEnvironment(DataSetCleanerParameters cleanerParameters) {
        return TargetIndexingDatabase.PUBLISH.toString().equals(cleanerParameters.getTargetIndexingEnv());
    }

    private boolean isPreviewEnvironment(DataSetCleanerParameters cleanerParameters) {
        return TargetIndexingDatabase.PREVIEW.toString().equals(cleanerParameters.getTargetIndexingEnv());
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
