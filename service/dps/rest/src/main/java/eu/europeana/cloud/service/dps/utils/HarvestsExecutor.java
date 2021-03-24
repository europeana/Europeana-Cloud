package eu.europeana.cloud.service.dps.utils;

import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.commons.urls.DataSetUrlParser;
import eu.europeana.cloud.service.dps.*;
import eu.europeana.cloud.service.dps.services.submitters.RecordSubmitService;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecordDAO;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.metis.harvesting.HarvesterException;
import eu.europeana.metis.harvesting.HarvesterFactory;
import eu.europeana.metis.harvesting.ReportingIteration.IterationResult;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvest;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvester;
import eu.europeana.metis.harvesting.oaipmh.OaiRecordHeader;
import eu.europeana.metis.harvesting.oaipmh.OaiRecordHeaderIterator;

import java.net.MalformedURLException;
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
    private final HarvestedRecordDAO harvestedRecordDAO;

    public HarvestsExecutor(RecordSubmitService recordSubmitService, TaskStatusChecker taskStatusChecker, HarvestedRecordDAO harvestedRecordDAO) {
        this.recordSubmitService = recordSubmitService;
        this.taskStatusChecker = taskStatusChecker;
        this.harvestedRecordDAO = harvestedRecordDAO;
    }

    public HarvestResult execute(List<OaiHarvest> harvestsToBeExecuted, SubmitTaskParameters parameters) throws HarvesterException {
        if(isIncremental(parameters) && parameters.getTaskParameter(PluginParameterKeys.SAMPLE_SIZE)!=null){
            //TODO impletent support for this or add such condition to TaskValidator
            throw new IllegalArgumentException("Incremental harvesting cound not set "+PluginParameterKeys.SAMPLE_SIZE);
        }
        final AtomicInteger resultCounter = new AtomicInteger(0);

        for (OaiHarvest harvest : harvestsToBeExecuted) {
            LOGGER.info("(Re-)starting identifiers harvesting for: {}. Task identifier: {}", harvest, parameters.getTask().getTaskId());
            OaiHarvester harvester = HarvesterFactory.createOaiHarvester(null, DEFAULT_RETRIES, SLEEP_TIME);
            OaiRecordHeaderIterator headerIterator = harvester.harvestRecordHeaders(harvest);

            // *** Main harvesting loop for given task ***
            final AtomicBoolean taskDropped = new AtomicBoolean(false);
            headerIterator.forEach(oaiHeader -> {
                if (taskStatusChecker.hasKillFlag(parameters.getTask().getTaskId())) {
                    LOGGER.info("Harvesting for {} (Task: {}) stopped by external signal", harvest, parameters.getTask().getTaskId());
                    taskDropped.set(true);
                    return IterationResult.TERMINATE;
                }
                executeRecord(parameters, resultCounter, harvest, oaiHeader);
                logProgressFor(harvest, parameters.incrementAndGetPerformedRecordCounter());
                return resultCounter.get() < getMaxRecordsCount(parameters)
                        ? IterationResult.CONTINUE : IterationResult.TERMINATE;
            });
            if (taskDropped.get()) {
                return HarvestResult.builder()
                        .resultCounter(resultCounter.get())
                        .taskState(TaskState.DROPPED).build();
            }
            LOGGER.info("Identifiers harvesting finished for: {}. Counter: {}", harvest, resultCounter);
        }

        if (isIncremental(parameters)) {
            detectDeletedRecords(parameters, resultCounter);
        }
        return new HarvestResult(resultCounter.get(), TaskState.QUEUED);
    }

    private void executeRecord(SubmitTaskParameters parameters, AtomicInteger resultCounter, OaiHarvest harvest, OaiRecordHeader oaiHeader) {
        if(oaiHeader.isDeleted()){
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

        DataSet dataSet = getSingleDataSet(parameters);
        Optional<HarvestedRecord> recordInDbOptional = harvestedRecordDAO.findRecord(dataSet.getProviderId(), dataSet.getId(), oaiHeader.getOaiIdentifier());
        if (recordInDbOptional.isEmpty()) {
            return false;
        }

        HarvestedRecord recordInDd = recordInDbOptional.get();
        return (recordInDd.getIndexingDate() != null && oaiHeader.getDatestamp().isBefore(recordInDd.getHarvestDate().toInstant()));
    }

    private void detectDeletedRecords(SubmitTaskParameters parameters, AtomicInteger resultCounter) {
        DataSet dataSet = getSingleDataSet(parameters);
        Iterator<HarvestedRecord> it = harvestedRecordDAO.findDatasetRecords(dataSet.getProviderId(), dataSet.getId());
        while (it.hasNext()) {
            HarvestedRecord record = it.next();
            if (record.getHarvestDate().before(parameters.getCurrentHarvestDate())) {
                executeDeleteRecord(parameters, resultCounter, record);
            }
        }
    }

    private void executeDeleteRecord(SubmitTaskParameters parameters, AtomicInteger resultCounter, HarvestedRecord record) {
        if (record.getIndexingDate() != null) {
            DpsRecord kafkaRecord = DpsRecord.builder()
                    .taskId(parameters.getTask().getTaskId())
                    .recordId(record.getRecordLocalId())
                    .markedAsDeleted(true)
                    .build();
            if (recordSubmitService.submitRecord(kafkaRecord, parameters)) {
                resultCounter.incrementAndGet();
            }
        } else {
            harvestedRecordDAO.deleteRecord(record.getProviderId(), record.getDatasetId(), record.getRecordLocalId());
        }
    }

    private void updateHarvestDate(SubmitTaskParameters parameters, OaiRecordHeader oaiHeader) {
        DataSet dataSet = getSingleDataSet(parameters);
        harvestedRecordDAO.updateHarvestDate(dataSet.getProviderId(), dataSet.getId(), oaiHeader.getOaiIdentifier(), parameters.getCurrentHarvestDate());
    }

    private DataSet getSingleDataSet(SubmitTaskParameters parameters) {
        List<DataSet> dataSetList = getDataSetList(parameters.getTask());
        if (dataSetList.size() != 1) {
            //TODO it could be checked in TaskValidator
            throw new IllegalArgumentException("IncrementalHarvesting is supported for one selected dataset!");
        }
        return dataSetList.get(0);
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
        for(DataSet dataset: getDataSetList(dpsTask)) {
            harvestedRecordDAO.insertHarvestedRecord(HarvestedRecord.builder()
                    .providerId(dataset.getProviderId())
                    .datasetId(dataset.getId())
                    .recordLocalId(oaiId)
                    .harvestDate(harvestDate)
                    .build());
        }
    }

    private List<DataSet> getDataSetList(DpsTask dpsTask) {
        try {
            return DataSetUrlParser.parseList(dpsTask.getParameter(PluginParameterKeys.OUTPUT_DATA_SETS));
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Invalid "+PluginParameterKeys.OUTPUT_DATA_SETS+" param value!",e);
        }
    }

}
