package eu.europeana.cloud.service.dps.utils;

import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.*;
import eu.europeana.cloud.service.dps.services.submitters.RecordSubmitService;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.metis.harvesting.HarvesterException;
import eu.europeana.metis.harvesting.HarvesterFactory;
import eu.europeana.metis.harvesting.ReportingIteration.IterationResult;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvest;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvester;
import eu.europeana.metis.harvesting.oaipmh.OaiRecordHeader;
import eu.europeana.metis.harvesting.oaipmh.OaiRecordHeaderIterator;
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

    public HarvestsExecutor(RecordSubmitService recordSubmitService, TaskStatusChecker taskStatusChecker) {
        this.recordSubmitService = recordSubmitService;
        this.taskStatusChecker = taskStatusChecker;
    }

    public HarvestResult execute(List<OaiHarvest> harvestsToBeExecuted, SubmitTaskParameters parameters) throws HarvesterException {
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
                DpsRecord record = convertToDpsRecord(oaiHeader, harvest, parameters.getTask());
                if (recordSubmitService.submitRecord(record, parameters)) {
                    resultCounter.incrementAndGet();
                }
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
        return new HarvestResult(resultCounter.get(), TaskState.QUEUED);
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
}
