package eu.europeana.cloud.service.dps.utils;

import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.*;
import eu.europeana.cloud.service.dps.oaipmh.Harvester;
import eu.europeana.cloud.service.dps.oaipmh.HarvesterException;
import eu.europeana.cloud.service.dps.oaipmh.HarvesterFactory;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.ProcessedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Iterator;
import java.util.List;

@Service
public class HarvestsExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(HarvestsExecutor.class);

    private static final int DEFAULT_RETRIES = 3;
    private static final int SLEEP_TIME = 5000;

    private final RecordExecutionSubmitService recordSubmitService;
    private final ProcessedRecordsDAO processedRecordsDAO;
    /**
     * Auxiliary object to check 'kill flag' for task
     */
    private final TaskStatusChecker taskStatusChecker;

    public HarvestsExecutor(RecordExecutionSubmitService recordSubmitService, ProcessedRecordsDAO processedRecordsDAO, TaskStatusChecker taskStatusChecker) {
        this.recordSubmitService = recordSubmitService;
        this.processedRecordsDAO = processedRecordsDAO;
        this.taskStatusChecker = taskStatusChecker;
    }

    public HarvestResult execute(List<Harvest> harvestsToBeExecuted, SubmitTaskParameters parameters) throws HarvesterException {
        int resultCounter = 0;

        for (Harvest harvest : harvestsToBeExecuted) {
            LOGGER.info("(Re-)starting identifiers harvesting for: {}. Task identifier: {}", harvest, parameters.getTask().getTaskId());
            Harvester harvester = HarvesterFactory.createHarvester(DEFAULT_RETRIES, SLEEP_TIME);
            Iterator<OAIHeader> headerIterator = harvester.harvestIdentifiers(harvest);

            // *** Main harvesting loop for given task ***
            while (headerIterator.hasNext()) {
                if (taskStatusChecker.hasKillFlag(parameters.getTask().getTaskId())) {
                    LOGGER.info("Harvesting for {} (Task: {}) stopped by external signal", harvest, parameters.getTask().getTaskId());
                    return HarvestResult.builder()
                            .resultCounter(resultCounter)
                            .taskState(TaskState.DROPPED).build();
                }

                OAIHeader oaiHeader = headerIterator.next();
                if (messageShouldBeEmitted(parameters, oaiHeader)) {
                    DpsRecord record = convertToDpsRecord(oaiHeader, harvest, parameters.getTask());
                    sendMessage(record, parameters.getTopicName());
                    updateRecordStatus(record, parameters.getTopologyName());
                    logProgressFor(harvest, resultCounter);
                    resultCounter++;
                }else{
                    LOGGER.info("Record {} will not be emitted because it was found in the 'processed_records' table", oaiHeader);
                }
            }
            LOGGER.info("Identifiers harvesting finished for: {}. Counter: {}", harvest, resultCounter);
        }
        return new HarvestResult(resultCounter, TaskState.QUEUED);
    }

    private boolean messageShouldBeEmitted(SubmitTaskParameters submitTaskParameters, OAIHeader oaiHeader){
        return
                !submitTaskParameters.isRestart()
                ||
                processedRecordsDAO.selectByPrimaryKey(submitTaskParameters.getTask().getTaskId(), oaiHeader.getIdentifier()).isEmpty();
    }

    /*package visiblility*/ DpsRecord convertToDpsRecord(OAIHeader oaiHeader, Harvest harvest, DpsTask dpsTask) {
        return DpsRecord.builder()
                .taskId(dpsTask.getTaskId())
                .recordId(oaiHeader.getIdentifier())
                .metadataPrefix(harvest.getMetadataPrefix())
                .build();
    }

    /*package visiblility*/ void sendMessage(DpsRecord record, String topicName) {
        LOGGER.debug("Sending records to messages queue: {}", record);
        recordSubmitService.submitRecord(record, topicName);
    }

    /*package visiblility*/  void updateRecordStatus(DpsRecord dpsRecord, String topologyName) {
        int attemptNumber = processedRecordsDAO.getAttemptNumber(dpsRecord.getTaskId(), dpsRecord.getRecordId());

        LOGGER.debug("Updating record in notifications table: {}", dpsRecord);
        processedRecordsDAO.insert(dpsRecord.getTaskId(), dpsRecord.getRecordId(), attemptNumber,
                "", topologyName, RecordState.QUEUED.toString(), "", "");
    }

    /*package visiblility*/ void logProgressFor(Harvest harvest, int counter) {
        if (counter % 1000 == 0) {
            LOGGER.info("Identifiers harvesting is progressing for: {}. Current counter: {}", harvest, counter);
        }
    }
}
