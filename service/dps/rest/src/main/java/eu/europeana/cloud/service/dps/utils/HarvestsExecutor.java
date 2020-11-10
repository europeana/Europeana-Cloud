package eu.europeana.cloud.service.dps.utils;

import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.*;
import eu.europeana.cloud.service.dps.oaipmh.Harvester;
import eu.europeana.cloud.service.dps.oaipmh.HarvesterException;
import eu.europeana.cloud.service.dps.oaipmh.HarvesterFactory;
import eu.europeana.cloud.service.dps.services.submitters.RecordSubmitService;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Iterator;
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

    public HarvestResult execute(List<Harvest> harvestsToBeExecuted, SubmitTaskParameters parameters) throws HarvesterException {
        int resultCounter = 0;

        for (Harvest harvest : harvestsToBeExecuted) {
            LOGGER.info("(Re-)starting identifiers harvesting for: {}. Task identifier: {}", harvest, parameters.getTask().getTaskId());
            Harvester harvester = HarvesterFactory.createHarvester(DEFAULT_RETRIES, SLEEP_TIME);
            Iterator<OAIHeader> headerIterator = harvester.harvestIdentifiers(harvest);

            // *** Main harvesting loop for given task ***
            while (headerIterator.hasNext() && resultCounter < getMaxRecordsCount(parameters)) {
                if (taskStatusChecker.hasKillFlag(parameters.getTask().getTaskId())) {
                    LOGGER.info("Harvesting for {} (Task: {}) stopped by external signal", harvest, parameters.getTask().getTaskId());
                    return HarvestResult.builder()
                            .resultCounter(resultCounter)
                            .taskState(TaskState.DROPPED).build();
                }

                OAIHeader oaiHeader = headerIterator.next();
                DpsRecord record = convertToDpsRecord(oaiHeader, harvest, parameters.getTask());
                if (recordSubmitService.submitRecord(record, parameters)) {
                    resultCounter++;
                }
                logProgressFor(harvest, parameters.incrementAndGetPerformedRecordCounter());
            }
            LOGGER.info("Identifiers harvesting finished for: {}. Counter: {}", harvest, resultCounter);
        }
        return new HarvestResult(resultCounter, TaskState.QUEUED);
    }

    private int getMaxRecordsCount(SubmitTaskParameters parameters) {
        return Optional.ofNullable(parameters.getTask().getParameter(PluginParameterKeys.SAMPLE_SIZE))  //return value SAMPLE_SIZE
                .map(Integer::parseInt)
                .orElse(Integer.MAX_VALUE);                                                             //or MAX_VALUE if null is above
    }

    /*package visiblility*/ DpsRecord convertToDpsRecord(OAIHeader oaiHeader, Harvest harvest, DpsTask dpsTask) {
        return DpsRecord.builder()
                .taskId(dpsTask.getTaskId())
                .recordId(oaiHeader.getIdentifier())
                .metadataPrefix(harvest.getMetadataPrefix())
                .build();
    }

    /*package visiblility*/ void logProgressFor(Harvest harvest, int counter) {
        if (counter % 1000 == 0) {
            LOGGER.info("Identifiers harvesting is progressing for: {}. Current counter: {}", harvest, counter);
        }
    }
}
