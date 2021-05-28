package eu.europeana.cloud.service.dps.utils;

import com.google.common.collect.Iterators;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.*;
import eu.europeana.cloud.service.dps.services.submitters.RecordSubmitService;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.metis.harvesting.HarvesterException;
import eu.europeana.metis.harvesting.HarvesterFactory;
import eu.europeana.metis.harvesting.ReportingIteration.IterationResult;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvest;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvester;
import eu.europeana.metis.harvesting.oaipmh.OaiRecordHeader;
import eu.europeana.metis.harvesting.oaipmh.OaiRecordHeaderIterator;

import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class HarvestsExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(HarvestsExecutor.class);

    private static final int DEFAULT_RETRIES = 3;
    private static final int SLEEP_TIME = 5000;

    private final RecordSubmitService recordSubmitService;

    /**
     * Auxiliary object to check 'kill flag' for task
     */
    private final TaskStatusChecker taskStatusChecker;
    private final HarvestedRecordsDAO harvestedRecordsDAO;

    public HarvestsExecutor(RecordSubmitService recordSubmitService, TaskStatusChecker taskStatusChecker, HarvestedRecordsDAO harvestedRecordsDAO) {
        this.recordSubmitService = recordSubmitService;
        this.taskStatusChecker = taskStatusChecker;
        this.harvestedRecordsDAO = harvestedRecordsDAO;
    }

    public HarvestResult execute(OaiHarvest harvestToBeExecuted, SubmitTaskParameters parameters) throws HarvesterException {
        final AtomicInteger resultCounter = new AtomicInteger(0);

        LOGGER.info("(Re-)starting identifiers harvesting for: {}. Task identifier: {}", harvestToBeExecuted, parameters.getTask().getTaskId());
        OaiHarvester harvester = HarvesterFactory.createOaiHarvester(null, DEFAULT_RETRIES, SLEEP_TIME);
        OaiRecordHeaderIterator headerIterator = harvester.harvestRecordHeaders(harvestToBeExecuted);

        // *** Main harvesting loop for given task ***
        final AtomicBoolean taskDropped = new AtomicBoolean(false);
        headerIterator.forEach(oaiHeader -> {
            if (taskStatusChecker.hasKillFlag(parameters.getTask().getTaskId())) {
                LOGGER.info("Harvesting for {} (Task: {}) stopped by external signal", harvestToBeExecuted, parameters.getTask().getTaskId());
                taskDropped.set(true);
                return IterationResult.TERMINATE;
            }
            executeRecord(parameters, resultCounter, harvestToBeExecuted, oaiHeader);
            logProgressFor(harvestToBeExecuted, parameters.incrementAndGetPerformedRecordCounter());
            return resultCounter.get() < getMaxRecordsCount(parameters)
                    ? IterationResult.CONTINUE : IterationResult.TERMINATE;
        });
        if (taskDropped.get()) {
            return HarvestResult.builder()
                    .resultCounter(resultCounter.get())
                    .taskState(TaskState.DROPPED).build();
        }
        LOGGER.info("Identifiers harvesting finished for: {}. Counter: {}", harvestToBeExecuted, resultCounter);


        if (isIncremental(parameters)) {
            emitDeletedRecords(parameters, resultCounter);
        }
        return new HarvestResult(resultCounter.get(), TaskState.QUEUED);
    }

    private void executeRecord(SubmitTaskParameters parameters, AtomicInteger resultCounter, OaiHarvest harvest, OaiRecordHeader oaiHeader) {
        if (oaiHeader.isDeleted()) {
            return;
        }

        if (shouldOmmitRecordCauseOfIncrementalHarvest(parameters, oaiHeader)) {
            updateHarvestDate(parameters, oaiHeader);
            return;
        }

        DpsRecord record = convertToDpsRecord(oaiHeader, harvest, parameters.getTask());
        if (recordSubmitService.submitRecord(record, parameters)) {
            resultCounter.incrementAndGet();
        }
        storeHarvestedRecord(oaiHeader.getOaiIdentifier(), parameters.getTask(), parameters.getCurrentHarvestDate());
    }

    private boolean shouldOmmitRecordCauseOfIncrementalHarvest(SubmitTaskParameters parameters, OaiRecordHeader oaiHeader) {
        if (!isIncremental(parameters)) {
            return false;
        }

        String metisDatasetId = parameters.getTaskParameter(PluginParameterKeys.METIS_DATASET_ID);
        Optional<HarvestedRecord> recordInDb = harvestedRecordsDAO.findRecord(metisDatasetId, oaiHeader.getOaiIdentifier());
        if (recordInDb.isEmpty()) {
            return false;
        }

        return isHeaderVersionAlreadyIndexed(oaiHeader, recordInDb.get());
    }

    private boolean isHeaderVersionAlreadyIndexed(OaiRecordHeader oaiHeader, HarvestedRecord recordInDd) {
        return recordInDd.getIndexingDate() != null && oaiHeader.getDatestamp().isBefore(recordInDd.getHarvestDate().toInstant());
    }

    private void emitDeletedRecords(SubmitTaskParameters parameters, AtomicInteger resultCounter) {
        Iterator<HarvestedRecord> it = fetchDeletedRecords(parameters);
        while (it.hasNext()) {
            HarvestedRecord record = it.next();
            emitDeletedRecord(parameters, resultCounter, record);
        }
    }

    private void emitDeletedRecord(SubmitTaskParameters parameters, AtomicInteger resultCounter, HarvestedRecord record) {
        DpsRecord kafkaRecord = DpsRecord.builder()
                .taskId(parameters.getTask().getTaskId())
                .recordId(record.getRecordLocalId())
                .markedAsDeleted(true)
                .build();
        if (recordSubmitService.submitRecord(kafkaRecord, parameters)) {
            resultCounter.incrementAndGet();
        }
    }

    private Iterator<HarvestedRecord> fetchDeletedRecords(SubmitTaskParameters parameters) {
        return Iterators.filter(
                harvestedRecordsDAO.findDatasetRecords(parameters.getTaskParameter(PluginParameterKeys.METIS_DATASET_ID))
                , record -> record.getHarvestDate().before(parameters.getCurrentHarvestDate())
        );
    }

    private void updateHarvestDate(SubmitTaskParameters parameters, OaiRecordHeader oaiHeader) {
        String metisDatasetId = parameters.getTaskParameter(PluginParameterKeys.METIS_DATASET_ID);
        harvestedRecordsDAO.updateHarvestDate(metisDatasetId, oaiHeader.getOaiIdentifier(), parameters.getCurrentHarvestDate());
    }

    private boolean isIncremental(SubmitTaskParameters parameters) {
        return "true".equals(parameters.getTaskParameter(PluginParameterKeys.INCREMENTAL_HARVEST));
    }

    private int getMaxRecordsCount(SubmitTaskParameters parameters) {
        return Optional.ofNullable(parameters.getTask().getParameter(PluginParameterKeys.SAMPLE_SIZE))  //return value SAMPLE_SIZE
                .map(Integer::parseInt)
                .orElse(Integer.MAX_VALUE);                                                             //or MAX_VALUE if null is above
    }

    /*package visiblility*/ DpsRecord convertToDpsRecord(OaiRecordHeader oaiHeader, OaiHarvest harvest, DpsTask dpsTask) {
        return DpsRecord.builder()
                .taskId(dpsTask.getTaskId())
                .recordId(oaiHeader.getOaiIdentifier())
                .metadataPrefix(harvest.getMetadataPrefix())
                .build();
    }

    /*package visiblility*/ void logProgressFor(OaiHarvest harvest, int counter) {
        if (counter % 1000 == 0) {
            LOGGER.info("Identifiers harvesting is progressing for: {}. Current counter: {}", harvest, counter);
        }
    }

    private void storeHarvestedRecord(String oaiId, DpsTask dpsTask, Date harvestDate) {
        String metisDatasetId = dpsTask.getParameter(PluginParameterKeys.METIS_DATASET_ID);
        harvestedRecordsDAO.insertHarvestedRecord(HarvestedRecord.builder()
                .metisDatasetId(metisDatasetId)
                .recordLocalId(oaiId)
                .harvestDate(harvestDate)
                .build());
    }

}
